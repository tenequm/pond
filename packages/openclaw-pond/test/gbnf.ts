// Static check that a tool parameter schema avoids JSON-schema features that
// break llama.cpp GBNF grammar generation (openclaw #108580). The grammar
// constrains the model's tool-call arguments, so only the `parameters` schema
// must stay safe. Flagged features: oneOf (and other combinators), format,
// patternProperties, plus $ref/conditionals that the grammar path cannot lower.
// TypeBox unions emit `anyOf`, which IS supported, so the plugin schemas pass.
//
// The walk is schema-position aware: a keyword like `format` is only a violation
// when it sits in a schema object, not when it is a property NAMED "format"
// under `properties`.

const FORBIDDEN_KEYWORDS = new Set([
  "oneOf",
  "allOf",
  "not",
  "if",
  "then",
  "else",
  "patternProperties",
  "format",
  "$ref",
  "dependencies",
  "dependentSchemas",
]);

// Keys whose values are maps of name -> schema; their keys are names, not keywords.
const NAMED_SCHEMA_CONTAINERS = new Set([
  "properties",
  "patternProperties",
  "$defs",
  "definitions",
  "dependentSchemas",
]);

export function findGbnfViolations(schema: unknown): string[] {
  const violations: string[] = [];

  const walkSchema = (node: unknown, path: string): void => {
    if (Array.isArray(node)) {
      node.forEach((item, index) => walkSchema(item, `${path}[${index}]`));
      return;
    }
    if (!node || typeof node !== "object") {
      return;
    }
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      if (FORBIDDEN_KEYWORDS.has(key)) {
        violations.push(`${path}/${key}`);
      }
      if (NAMED_SCHEMA_CONTAINERS.has(key) && value && typeof value === "object") {
        for (const [name, child] of Object.entries(value as Record<string, unknown>)) {
          walkSchema(child, `${path}/${key}/${name}`);
        }
        continue;
      }
      walkSchema(value, `${path}/${key}`);
    }
  };

  // Strip TypeBox symbol keys by round-tripping through JSON.
  walkSchema(JSON.parse(JSON.stringify(schema)), "$");
  return violations;
}
