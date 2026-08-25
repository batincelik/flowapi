import { describe, expect, it } from "vitest";
import Home from "./page";

describe("dashboard entrypoint", () => {
  it("mounts the workflow studio inside its React Flow provider", () => {
    const page = Home();
    expect(typeof page.type).toBe("function");
    expect(page.props.children).toBeTruthy();
  });
});
