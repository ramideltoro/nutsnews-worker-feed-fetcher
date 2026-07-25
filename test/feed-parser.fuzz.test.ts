import {
  describe,
  expect,
  it
} from "vitest";

import {
  FeedParseError,
  parseFeedXml
} from "../src/feed-parser.js";

describe("parseFeedXml hostile input coverage", () => {
  it("bounds parser outcomes for deterministic fuzz cases", () => {
    for (const xml of generatedXmlCases()) {
      try {
        const parsed = parseFeedXml(xml, "https://feeds.example.test/base/feed.xml", "feed-fuzz");

        expect(parsed.sourceName.length).toBeLessThanOrEqual(2_048);

        for (const item of parsed.items) {
          expect(item.title.length).toBeLessThanOrEqual(2_048);
          expect(item.excerpt?.length ?? 0).toBeLessThanOrEqual(512);
          expect(item.originalUrl).toMatch(/^https?:\/\//u);
          expect(item.canonicalUrl).toMatch(/^https?:\/\//u);
        }
      } catch (error: unknown) {
        expect(error).toBeInstanceOf(FeedParseError);
      }
    }
  });
});

function generatedXmlCases(): readonly string[] {
  const baseCases = [
    "",
    "not xml",
    "<rss>",
    "<rss><channel></rss>",
    "<feed><entry><link href='relative'></entry></feed>",
    `<rss><channel><title>${"A".repeat(3_000)}</title><item><title>${"B".repeat(3_000)}</title><link>/story</link><description>${"C".repeat(800)}</description></item></channel></rss>`,
    "<rss><channel><title><![CDATA[Source <b>One</b>]]></title><item><title><![CDATA[Story & Stuff]]></title><link>/story</link></item></channel></rss>"
  ];
  const generated: string[] = [];
  let state = 0x5eed;

  for (let index = 0; index < 48; index += 1) {
    state = nextState(state);
    const depth = 1 + (state % 8);
    state = nextState(state);
    const includeClose = state % 3 !== 0;
    state = nextState(state);
    const textLength = state % 256;
    const text = fuzzText(textLength, state);
    const openTags = Array.from({
      length: depth
    }, (_entry, tagIndex) => `<n${String(tagIndex)}>`);
    const closeTags = Array.from({
      length: includeClose ? depth : Math.floor(depth / 2)
    }, (_entry, tagIndex) => `</n${String(depth - tagIndex - 1)}>`);

    generated.push(`${openTags.join("")}${text}${closeTags.join("")}`);
  }

  return [
    ...baseCases,
    ...generated
  ];
}

function nextState(value: number): number {
  return (value * 1_103_515_245 + 12_345) % 0x7fffffff;
}

function fuzzText(length: number, seed: number): string {
  const alphabet = "abc 123 <> & \" ' \n \t [] {}";
  let state = seed;
  let output = "";

  for (let index = 0; index < length; index += 1) {
    state = nextState(state);
    const char = alphabet[state % alphabet.length];

    if (char !== undefined) {
      output += char;
    }
  }

  return output;
}
