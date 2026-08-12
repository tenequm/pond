// The `/pond` picker. Split into its own module so the TUI component libraries
// load only when a real terminal session opens the picker - headless pi runs
// (json/print/rpc) never import them.
//
// Two actions on one list, which is why this is `ctx.ui.custom()` and not
// `ctx.ui.select()`: Enter resumes the session (switch pi to it), `i` pastes a
// compact reference into the editor so the model can pull detail through the
// tools instead of having a transcript force-fed to it.
import { DynamicBorder } from "@earendil-works/pi-coding-agent";
import type { ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import { Container, type SelectItem, SelectList, Text } from "@earendil-works/pi-tui";
import { hitLabel, type PondHit } from "./hits.ts";

export type PickerChoice = { action: "resume" | "insert"; hit: PondHit } | undefined;

const MAX_VISIBLE = 10;

export async function pickPondHit(
  ctx: ExtensionCommandContext,
  hits: PondHit[],
): Promise<PickerChoice> {
  const items: SelectItem[] = hits.map((hit, index) => ({
    value: String(index),
    label: hitLabel(hit),
    description: hit.snippet,
  }));

  const chosen = await ctx.ui.custom<{ action: "resume" | "insert"; index: number } | undefined>(
    (tui, theme, _keybindings, done) => {
      const container = new Container();
      container.addChild(new DynamicBorder((s: string) => theme.fg("accent", s)));
      container.addChild(
        new Text(theme.fg("accent", theme.bold(`pond - ${hits.length} session(s)`)), 1, 0),
      );

      const list = new SelectList(items, Math.min(items.length, MAX_VISIBLE), {
        selectedPrefix: (t: string) => theme.fg("accent", t),
        selectedText: (t: string) => theme.fg("accent", t),
        description: (t: string) => theme.fg("muted", t),
        scrollInfo: (t: string) => theme.fg("dim", t),
        noMatch: (t: string) => theme.fg("warning", t),
      });
      // SelectList keeps its index private; track the highlight as it moves so
      // the `i` action knows which row it applies to.
      let highlighted = 0;
      list.onSelectionChange = (item: SelectItem) => {
        highlighted = Number(item.value);
      };
      list.onSelect = (item: SelectItem) => done({ action: "resume", index: Number(item.value) });
      list.onCancel = () => done(undefined);
      container.addChild(list);
      container.addChild(
        new Text(
          theme.fg("dim", "enter resume here | i insert as context | esc cancel"),
          1,
          0,
        ),
      );
      container.addChild(new DynamicBorder((s: string) => theme.fg("accent", s)));

      return {
        render: (width: number) => container.render(width),
        invalidate: () => container.invalidate(),
        handleInput: (data: string) => {
          if (data === "i") {
            done({ action: "insert", index: highlighted });
            return;
          }
          list.handleInput(data);
          tui.requestRender();
        },
      };
    },
  );

  if (!chosen) {
    return undefined;
  }
  const hit = hits[chosen.index];
  return hit ? { action: chosen.action, hit } : undefined;
}
