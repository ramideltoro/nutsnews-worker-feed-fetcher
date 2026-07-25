import {
  describe,
  expect,
  it
} from "vitest";

import { parseFeedXml } from "../src/feed-parser.js";

describe("parseFeedXml", () => {
  it("parses RSS with namespaces, CDATA, relative links, image hints, and malformed dates", () => {
    const parsed = parseFeedXml(`<?xml version="1.0"?>
      <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:content="http://purl.org/rss/1.0/modules/content/">
        <channel>
          <title>World Source</title>
          <language>en-US</language>
          <item>
            <guid isPermaLink="false">guid-001</guid>
            <title><![CDATA[ Story <One> ]]></title>
            <link>/story-one</link>
            <pubDate>not a date</pubDate>
            <description><![CDATA[<p>Summary with <b>markup</b>.</p>]]></description>
            <media:thumbnail url="/img/story-one.jpg" />
          </item>
        </channel>
      </rss>`, "https://feeds.example.test/world/feed.xml", "feed-world");

    expect(parsed).toMatchObject({
      format: "rss",
      sourceName: "World Source",
      language: "en-US"
    });
    expect(parsed.items).toHaveLength(1);
    expect(parsed.items[0]).toMatchObject({
      sourceItemId: "guid-001",
      originalUrl: "https://feeds.example.test/story-one",
      canonicalUrl: "https://feeds.example.test/story-one",
      title: "Story <One>",
      excerpt: "Summary with markup.",
      imageUrl: "https://feeds.example.test/img/story-one.jpg",
      language: "en-US"
    });
    expect(parsed.items[0]?.publishedAt).toBeUndefined();
  });

  it("parses Atom with canonical links, language, and valid dates", () => {
    const parsed = parseFeedXml(`<?xml version="1.0" encoding="UTF-8"?>
      <feed xmlns="http://www.w3.org/2005/Atom" xml:lang="es">
        <title>Atom Source</title>
        <entry>
          <id>tag:example.test,2026:story-2</id>
          <title>Historia dos</title>
          <link rel="alternate" href="../article/two" />
          <link rel="canonical" href="https://articles.example.test/two" />
          <published>2026-07-23T04:05:06Z</published>
          <summary>Resumen</summary>
        </entry>
      </feed>`, "https://feeds.example.test/world/feed.xml", "feed-world");

    expect(parsed).toMatchObject({
      format: "atom",
      sourceName: "Atom Source",
      language: "es"
    });
    expect(parsed.items).toHaveLength(1);
    expect(parsed.items[0]).toMatchObject({
      sourceItemId: "tag:example.test,2026:story-2",
      originalUrl: "https://feeds.example.test/article/two",
      canonicalUrl: "https://articles.example.test/two",
      title: "Historia dos",
      publishedAt: "2026-07-23T04:05:06.000Z",
      excerpt: "Resumen",
      language: "es"
    });
  });

  it("returns an empty candidate list for an empty RSS feed", () => {
    const parsed = parseFeedXml("<rss><channel><title>Empty</title></channel></rss>", "https://feeds.example.test/empty.xml", "feed-empty");

    expect(parsed.items).toEqual([]);
  });
});
