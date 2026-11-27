import { test, expect } from "bun:test";
import { rpcDeliveryCommand } from "./omp-supervisor-agent.mjs";

test("busy sessions use prompt streamingBehavior and copy only a real agent source", () => {
  expect(rpcDeliveryCommand({ text: "same text", mode: "follow_up", from: "Label" }, true)).toEqual({
    type: "prompt",
    message: "same text",
    streamingBehavior: "followUp",
  });
  expect(rpcDeliveryCommand({
    text: "urgent",
    mode: "steer",
    source: { kind: "agent", from: "Peer", name: "Researcher", title: "Observed task", route: "supervisor", extra: true },
  }, true)).toEqual({
    type: "prompt",
    message: "urgent",
    streamingBehavior: "steer",
    source: { kind: "agent", from: "Peer", name: "Researcher", title: "Observed task", route: "supervisor" },
  });
  expect(rpcDeliveryCommand({ text: "hello", mode: "steer", source: null }, true)).toEqual({
    type: "prompt",
    message: "hello",
    streamingBehavior: "steer",
  });
  const idle = rpcDeliveryCommand({ text: "hello", mode: "follow_up", source: { kind: "agent", from: "Peer" } }, false);
  expect(idle).toEqual({ type: "prompt", message: "hello", source: { kind: "agent", from: "Peer" } });
  expect("streamingBehavior" in idle).toBe(false);
  const unmarked = rpcDeliveryCommand({ text: "hello", mode: "steer" }, false);
  expect(unmarked).toEqual({ type: "prompt", message: "hello" });
  expect("source" in unmarked).toBe(false);
  for (const source of [{ kind: "agent", from: "  " }, { kind: "human", from: "Pat" }, "Peer", { kind: "agent", from: "Peer", name: 1 }, { kind: "agent", from: "Peer", route: "coding" }]) {
    expect(() => rpcDeliveryCommand({ text: "x", mode: "steer", source }, true)).toThrow("Invalid agent message source");
  }
});

test("explicit aside stays on the RPC wire even after an idle snapshot", () => {
  const peer = { kind: "agent", from: "sub_worker", name: "Researcher", title: "Observed task", route: "omp" };
  for (const source of [undefined, peer]) {
    for (const busy of [false, true]) {
      expect(rpcDeliveryCommand({ text: "peer input", mode: "aside", source }, busy)).toEqual({
        type: "prompt",
        message: "peer input",
        streamingBehavior: "aside",
        ...(source ? { source } : {}),
      });
    }
  }
});
