package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var captureGzipChunk = 4 << 20

var captureGzipSubfield = [2]byte{'S', 'C'}

func captureGzipPath(logPath string) string { return logPath + ".gz" }

type captureSource interface {
	Size() int64

	ReaderFrom(off int64) (io.Reader, error)
	Close() error
}

type plainCapture struct {
	file *os.File
	size int64
}

func (p *plainCapture) Size() int64 { return p.size }
func (p *plainCapture) ReaderFrom(off int64) (io.Reader, error) {
	return io.NewSectionReader(p.file, off, p.size-off), nil
}
func (p *plainCapture) Close() error { return p.file.Close() }

type gzipMember struct {
	gzOff, gzLen, rawOff, rawLen int64
}

type gzipCapture struct {
	file    *os.File
	gzSize  int64
	size    int64
	members []gzipMember
}

func (g *gzipCapture) Size() int64  { return g.size }
func (g *gzipCapture) Close() error { return g.file.Close() }

func (g *gzipCapture) ReaderFrom(off int64) (io.Reader, error) {
	if off >= g.size {
		return bytes.NewReader(nil), nil
	}
	start := gzipMember{}
	if g.members != nil {
		for _, m := range g.members {
			if off < m.rawOff+m.rawLen || m.rawOff+m.rawLen == g.size {
				start = m
				break
			}
		}
	}
	zr, err := gzip.NewReader(io.NewSectionReader(g.file, start.gzOff, g.gzSize-start.gzOff))
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, zr, off-start.rawOff); err != nil {
		return nil, err
	}
	return zr, nil
}

func openCapture(logPath string) (captureSource, error) {
	file, err := os.Open(logPath)
	if err == nil {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() {
			file.Close()
			return nil, fmt.Errorf("RPC log is not a regular file")
		}
		return &plainCapture{file: file, size: info.Size()}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	gzFile, gzErr := os.Open(captureGzipPath(logPath))
	if gzErr != nil {

		return nil, err
	}
	capture, err := indexGzipCapture(gzFile)
	if err != nil {
		gzFile.Close()
		return nil, fmt.Errorf("compressed RPC log: %w", err)
	}
	return capture, nil
}

func indexGzipCapture(file *os.File) (*gzipCapture, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file")
	}
	g := &gzipCapture{file: file, gzSize: info.Size()}

	var zr gzip.Reader
	br := bufio.NewReader(nil)
	resetAt := func(off int64) error {
		br.Reset(io.NewSectionReader(file, off, g.gzSize-off))
		return zr.Reset(br)
	}
	var members []gzipMember
	var gzOff, rawOff int64
	for gzOff < g.gzSize {
		if err := resetAt(gzOff); err != nil {
			return nil, err
		}
		gzLen, rawLen, ok := parseCaptureExtra(zr.Header.Extra)
		if !ok || gzLen <= 0 || gzOff+gzLen > g.gzSize {
			members = nil
			break
		}
		members = append(members, gzipMember{gzOff: gzOff, gzLen: gzLen, rawOff: rawOff, rawLen: rawLen})
		gzOff += gzLen
		rawOff += rawLen
	}
	if members != nil || g.gzSize == 0 {
		g.members, g.size = members, rawOff
		return g, nil
	}

	if err := resetAt(0); err != nil {
		return nil, err
	}
	g.size, err = io.Copy(io.Discard, &zr)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func captureExtra(gzLen, rawLen int64) []byte {
	extra := make([]byte, 4+16)
	extra[0], extra[1] = captureGzipSubfield[0], captureGzipSubfield[1]
	binary.LittleEndian.PutUint16(extra[2:], 16)
	binary.LittleEndian.PutUint64(extra[4:], uint64(gzLen))
	binary.LittleEndian.PutUint64(extra[12:], uint64(rawLen))
	return extra
}

func parseCaptureExtra(extra []byte) (gzLen, rawLen int64, ok bool) {
	for len(extra) >= 4 {
		size := int(binary.LittleEndian.Uint16(extra[2:]))
		if len(extra) < 4+size {
			return 0, 0, false
		}
		if extra[0] == captureGzipSubfield[0] && extra[1] == captureGzipSubfield[1] && size == 16 {
			return int64(binary.LittleEndian.Uint64(extra[4:])), int64(binary.LittleEndian.Uint64(extra[12:])), true
		}
		extra = extra[4+size:]
	}
	return 0, 0, false
}

func writeCaptureGzip(dst io.Writer, src io.Reader) ([]byte, int64, error) {
	hash := sha256.New()
	reader := io.TeeReader(src, hash)
	chunk := make([]byte, captureGzipChunk)
	var member bytes.Buffer
	var total int64
	for {
		n, err := io.ReadFull(reader, chunk)
		if n > 0 {
			member.Reset()

			zw, werr := gzip.NewWriterLevel(&member, gzip.DefaultCompression)
			if werr != nil {
				return nil, 0, werr
			}
			zw.Header = gzip.Header{Extra: captureExtra(0, int64(n)), OS: 255}
			if _, werr := zw.Write(chunk[:n]); werr != nil {
				return nil, 0, werr
			}
			if werr := zw.Close(); werr != nil {
				return nil, 0, werr
			}
			body := member.Bytes()

			patched := captureExtra(int64(len(body)), int64(n))
			if len(body) < 12+len(patched) || body[3]&0x04 == 0 ||
				int(binary.LittleEndian.Uint16(body[10:])) != len(patched) || !bytes.Equal(body[12:16], patched[:4]) {
				return nil, 0, fmt.Errorf("unexpected gzip header layout")
			}
			copy(body[12:], patched)
			if _, werr := dst.Write(body); werr != nil {
				return nil, 0, werr
			}
			total += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return hash.Sum(nil), total, nil
		}
		if err != nil {
			return nil, 0, err
		}
	}
}

func captureCompressionEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_COMPRESS"))) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func compressFinishedCapture(store *JobStore, job *Job) error {
	job.archiveMu.Lock()
	defer job.archiveMu.Unlock()
	job.mu.RLock()
	steerable, logPath, id := job.Steerable, job.LogPath, job.ID
	running := !jobTerminal(job.Status) || !captureDrained(job) || job.rpc != nil
	job.mu.RUnlock()
	if !steerable || logPath == "" {
		return fmt.Errorf("capture compression refused: job has no RPC capture")
	}
	if running {
		return fmt.Errorf("capture compression refused: capture is still active")
	}
	if err := jobLogOwned(store, id, logPath); err != nil {
		return fmt.Errorf("capture compression refused: %w", err)
	}
	gzPath := captureGzipPath(logPath)
	src, err := openRegular(logPath)
	if errors.Is(err, os.ErrNotExist) {
		return repackCaptureGzip(gzPath)
	}
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	if gzInfo, err := os.Lstat(gzPath); err == nil && gzInfo.Mode().IsRegular() {

		hash := sha256.New()
		n, err := io.Copy(hash, src)
		if err != nil {
			return err
		}
		if verifyCaptureGzip(gzPath, hash.Sum(nil), n) == nil {
			return removeCompressedLog(src, logPath)
		}
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	err = installCaptureGzip(gzPath, src, func(rawSize int64) error {
		if info, err := src.Stat(); err != nil || info.Size() != rawSize {
			return fmt.Errorf("capture compression refused: log changed while compressing")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return removeCompressedLog(src, logPath)
}

func installCaptureGzip(gzPath string, src io.Reader, check func(rawSize int64) error) error {
	tmp, err := os.CreateTemp(filepath.Dir(gzPath), "."+filepath.Base(gzPath)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	buffered := bufio.NewWriterSize(tmp, 1<<20)
	rawSum, rawSize, err := writeCaptureGzip(buffered, src)
	if err == nil {
		err = buffered.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("capture compression failed: %w", err)
	}
	if err := check(rawSize); err != nil {
		return err
	}
	if err := verifyCaptureGzip(tmpName, rawSum, rawSize); err != nil {
		return fmt.Errorf("capture compression failed: %w", err)
	}
	if err := os.Rename(tmpName, gzPath); err != nil {
		return err
	}
	return syncDir(filepath.Dir(gzPath))
}

func removeCompressedLog(src *os.File, logPath string) error {
	src.Close()
	if err := os.Remove(logPath); err != nil {
		return fmt.Errorf("capture compressed, raw log removal will be retried: %w", err)
	}
	return syncDir(filepath.Dir(logPath))
}

func repackCaptureGzip(gzPath string) error {
	file, err := openRegular(gzPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	capture, err := indexGzipCapture(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("capture repack failed: %w", err)
	}
	defer capture.Close()
	if capture.members != nil || capture.size == 0 {
		return nil
	}
	src, err := capture.ReaderFrom(0)
	if err != nil {
		return fmt.Errorf("capture repack failed: %w", err)
	}
	return installCaptureGzip(gzPath, src, func(rawSize int64) error {
		info, err := capture.file.Stat()
		if err != nil || info.Size() != capture.gzSize || rawSize != capture.size {
			return fmt.Errorf("capture repack refused: compressed capture changed while repacking")
		}

		return capture.Close()
	})
}

func verifyCaptureGzip(path string, rawSum []byte, rawSize int64) error {
	file, err := openRegular(path)
	if err != nil {
		return err
	}
	capture, err := indexGzipCapture(file)
	if err != nil {
		file.Close()
		return err
	}
	defer capture.Close()
	if capture.members == nil {
		return fmt.Errorf("compressed capture has no member index")
	}
	reader, err := capture.ReaderFrom(0)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, reader)
	if err != nil {
		return err
	}
	if n != rawSize || capture.size != rawSize || !bytes.Equal(hash.Sum(nil), rawSum) {
		return fmt.Errorf("compressed capture does not match the original")
	}
	return nil
}

func compressCaptureLogged(store *JobStore, job *Job) {
	if !captureCompressionEnabled() {
		return
	}
	if err := compressFinishedCapture(store, job); err != nil {
		log.Printf("job %s: %v; raw capture retained", job.ID, err)
	}
}

func compressFinishedCaptures(store *JobStore) {
	if store == nil {
		return
	}
	removeOrphanedCaptureTemps(store.logsDir)
	if !captureCompressionEnabled() {
		return
	}
	store.mu.RLock()
	jobs := make([]*Job, 0, len(store.jobs))
	for _, job := range store.jobs {
		jobs = append(jobs, job)
	}
	store.mu.RUnlock()
	for _, job := range jobs {
		job.mu.RLock()
		eligible := job.Steerable && job.LogPath != "" && jobTerminal(job.Status) && captureDrained(job) && job.rpc == nil
		logPath := job.LogPath
		job.mu.RUnlock()
		if !eligible {
			continue
		}
		if info, err := os.Lstat(logPath); err == nil {
			if !info.Mode().IsRegular() {
				continue
			}
		} else if info, err := os.Lstat(captureGzipPath(logPath)); err != nil || !info.Mode().IsRegular() {
			continue
		}
		if err := compressFinishedCapture(store, job); err != nil {
			log.Printf("job %s: %v; capture retained as is", job.ID, err)
		}
	}
}

func removeOrphanedCaptureTemps(logsDir string) {
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("capture temp cleanup: %v", err)
		}
		return
	}
	for _, entry := range entries {
		if ok, _ := filepath.Match(".*.log.gz.tmp-*", entry.Name()); !ok || !entry.Type().IsRegular() {
			continue
		}
		if err := os.Remove(filepath.Join(logsDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("capture temp cleanup: %v", err)
		}
	}
}

func hashLocalCapture(logPath string) (sum string, size int64, compressed bool, err error) {
	sum, size, err = hashFile(logPath)
	if !errors.Is(err, os.ErrNotExist) {
		return sum, size, false, err
	}
	file, gzErr := openRegular(captureGzipPath(logPath))
	if gzErr != nil {
		if errors.Is(gzErr, os.ErrNotExist) {
			return "", 0, false, err
		}
		return "", 0, true, gzErr
	}
	capture, gzErr := indexGzipCapture(file)
	if gzErr != nil {
		file.Close()
		return "", 0, true, gzErr
	}
	defer capture.Close()
	reader, gzErr := capture.ReaderFrom(0)
	if gzErr != nil {
		return "", 0, true, gzErr
	}
	hash := sha256.New()
	size, gzErr = io.Copy(hash, reader)
	if gzErr != nil {
		return "", 0, true, gzErr
	}
	return hex.EncodeToString(hash.Sum(nil)), size, true, nil
}

func localCapturePresent(logPath string) bool {
	for _, p := range []string{logPath, captureGzipPath(logPath)} {
		if info, err := os.Lstat(p); err == nil && info.Mode().IsRegular() {
			return true
		}
	}
	return false
}
