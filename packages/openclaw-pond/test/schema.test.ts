import { Type } from "typebox";
import { describe, expect, it } from "vitest";
import {
  GetMessageParamsSchema,
  GetSessionParamsSchema,
  SearchParamsSchema,
  SqlParamsSchema,
  ToolOutputSchema,
} from "../src/schemas.js";
import { findGbnfViolations } from "./gbnf.js";

describe("tool parameter schemas are GBNF-safe", () => {
  const cases = [
    ["pond_search", SearchParamsSchema],
    ["pond_get_session", GetSessionParamsSchema],
    ["pond_get_message", GetMessageParamsSchema],
    ["pond_sql", SqlParamsSchema],
  ] as const;

  for (const [name, schema] of cases) {
    it(`${name} avoids grammar-breaking keywords`, () => {
      expect(findGbnfViolations(schema)).toEqual([]);
    });
  }

  it("uses anyOf (not oneOf) for unions", () => {
    const json = JSON.stringify(SearchParamsSchema);
    expect(json).toContain("anyOf");
    expect(json).not.toContain("oneOf");
  });

  it("the checker actually catches forbidden features (negative control)", () => {
    const unsafe = Type.Object({
      // oneOf + format are exactly what #108580 flagged.
      when: Type.Union([Type.Literal("a"), Type.Literal("b")]),
      stamp: Type.String({ format: "date-time" }),
    });
    // Force a top-level oneOf to prove detection.
    const withOneOf = { ...JSON.parse(JSON.stringify(unsafe)), oneOf: [{ type: "object" }] };
    const violations = findGbnfViolations(withOneOf);
    expect(violations).toContain("$/oneOf");
    expect(violations.some((path) => path.endsWith("/format"))).toBe(true);
  });

  it("the output union is itself GBNF-safe (anyOf only)", () => {
    // Output schemas are not grammar-constrained, but keeping them clean avoids
    // surprises if a runtime ever validates them through the same path.
    expect(findGbnfViolations(ToolOutputSchema)).toEqual([]);
  });
});
