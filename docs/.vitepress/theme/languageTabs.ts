// Keeps the code groups of a page on one language: choosing Python in one group switches every
// group that has a Python tab, and the choice is remembered for the next pages. A tab's language
// comes from its label: "TypeScript" or a `.ts` file, "Python" or a `.py` file, "Rust" or a
// `.rs` file.
import { inBrowser, onContentUpdated } from "vitepress";

const KEY = "kernable-language";
const LANGUAGES: [string, RegExp][] = [
  ["ts", /^TypeScript$|\.ts$/],
  ["py", /^Python$|\.py$/],
  ["rs", /^Rust$|\.rs$/],
];

function languageOf(label: string): string | undefined {
  return LANGUAGES.find(([, re]) => re.test(label.trim()))?.[0];
}

/** Selects the tab of `language` in every code group that has one. */
function select(language: string): void {
  document.querySelectorAll(".vp-code-group").forEach((group) => {
    const inputs = Array.from(group.querySelectorAll<HTMLInputElement>(".tabs input"));
    const i = inputs.findIndex((input) => {
      const label = group.querySelector(`label[for="${input.id}"]`);
      return label && languageOf(label.textContent ?? "") === language;
    });
    const blocks = group.querySelector(".blocks")?.children;
    if (i < 0 || !blocks?.[i]) return;
    inputs[i].checked = true;
    Array.from(blocks).forEach((block, j) => block.classList.toggle("active", j === i));
  });
}

function remembered(): string | undefined {
  try {
    return localStorage.getItem(KEY) ?? undefined;
  } catch {
    return undefined;
  }
}

export function useLanguageTabs(): void {
  // After VitePress has set up the groups of new content.
  onContentUpdated(() => {
    const language = remembered();
    if (language) setTimeout(() => select(language));
  });
  if (!inBrowser) return;
  window.addEventListener("click", (e) => {
    const input = e.target as HTMLElement;
    if (!input.matches?.(".vp-code-group input")) return;
    const label = input.parentElement?.querySelector(`label[for="${input.id}"]`);
    const language = languageOf(label?.textContent ?? "");
    if (!language) return;
    // Groups above the clicked one change height when they switch; keep the clicked tab where
    // it is on screen instead of letting the page jump.
    const top = input.getBoundingClientRect().top;
    select(language);
    window.scrollBy(0, input.getBoundingClientRect().top - top);
    try {
      localStorage.setItem(KEY, language);
    } catch {
      // Not remembered without storage; the page still switches.
    }
  });
}
