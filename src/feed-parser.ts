import { XMLParser } from "fast-xml-parser";

export interface ParsedFeed {
  readonly format: "rss" | "atom";
  readonly sourceName: string;
  readonly language?: string;
  readonly items: readonly ParsedFeedItem[];
}

export interface ParsedFeedItem {
  readonly sourceItemId: string;
  readonly originalUrl: string;
  readonly canonicalUrl: string;
  readonly title: string;
  readonly sourceName: string;
  readonly publishedAt?: string;
  readonly excerpt?: string;
  readonly imageUrl?: string;
  readonly language?: string;
}

export class FeedParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FeedParseError";
  }
}

const parser = new XMLParser({
  attributeNamePrefix: "@",
  cdataPropName: "#cdata",
  ignoreAttributes: false,
  parseAttributeValue: false,
  parseTagValue: false,
  removeNSPrefix: true,
  textNodeName: "#text",
  trimValues: true
});

const MAX_TEXT_LENGTH = 2_048;
const MAX_EXCERPT_LENGTH = 512;

export function parseFeedXml(xml: string, feedUrl: string, feedId: string): ParsedFeed {
  const root = asRecord(parser.parse(xml));

  if (root === undefined) {
    throw new FeedParseError("Feed XML did not parse to an object.");
  }

  const rss = asRecord(root.rss);

  if (rss !== undefined) {
    return parseRss(rss, feedUrl, feedId);
  }

  const feed = asRecord(root.feed);

  if (feed !== undefined) {
    return parseAtom(feed, feedUrl, feedId);
  }

  throw new FeedParseError("Unsupported feed XML root.");
}

function parseRss(rss: XmlRecord, feedUrl: string, feedId: string): ParsedFeed {
  const channel = firstRecord(rss.channel);

  if (channel === undefined) {
    throw new FeedParseError("RSS feed is missing channel.");
  }

  const sourceName = boundedText(textValue(channel.title), feedId);
  const language = optionalText(channel.language);
  const items = records(channel.item).map((item) => rssItem(item, feedUrl, sourceName, language));

  return {
    format: "rss",
    sourceName,
    ...(language === undefined ? {} : {
      language
    }),
    items: items.filter((item): item is ParsedFeedItem => item !== undefined)
  };
}

function rssItem(
  item: XmlRecord,
  feedUrl: string,
  sourceName: string,
  feedLanguage: string | undefined
): ParsedFeedItem | undefined {
  const originalUrl = resolveUrl(optionalText(item.link), feedUrl);
  const canonicalUrl = resolveUrl(optionalText(item.canonical) ?? optionalText(item.link), feedUrl) ?? originalUrl;

  if (originalUrl === undefined || canonicalUrl === undefined) {
    return undefined;
  }

  const sourceItemId = boundedText(optionalText(item.guid) ?? optionalText(item.id) ?? canonicalUrl, canonicalUrl);
  const title = boundedText(optionalText(item.title), canonicalUrl);
  const publishedAt = parseOptionalDate(optionalText(item.pubDate) ?? optionalText(item.published) ?? optionalText(item.updated) ?? optionalText(item.date));
  const excerpt = excerptText(optionalText(item.description) ?? optionalText(item.summary) ?? optionalText(item.encoded));
  const imageUrl = imageHint(item, feedUrl);
  const language = optionalText(item.language) ?? feedLanguage;

  return {
    sourceItemId,
    originalUrl,
    canonicalUrl,
    title,
    sourceName,
    ...(publishedAt === undefined ? {} : {
      publishedAt
    }),
    ...(excerpt === undefined ? {} : {
      excerpt
    }),
    ...(imageUrl === undefined ? {} : {
      imageUrl
    }),
    ...(language === undefined ? {} : {
      language
    })
  };
}

function parseAtom(feed: XmlRecord, feedUrl: string, feedId: string): ParsedFeed {
  const sourceName = boundedText(textValue(feed.title), feedId);
  const language = attr(feed, "lang");
  const items = records(feed.entry).map((entry) => atomEntry(entry, feedUrl, sourceName, language));

  return {
    format: "atom",
    sourceName,
    ...(language === undefined ? {} : {
      language
    }),
    items: items.filter((item): item is ParsedFeedItem => item !== undefined)
  };
}

function atomEntry(
  entry: XmlRecord,
  feedUrl: string,
  sourceName: string,
  feedLanguage: string | undefined
): ParsedFeedItem | undefined {
  const alternate = atomLink(entry.link, "alternate") ?? atomLink(entry.link, undefined);
  const canonical = atomLink(entry.link, "canonical") ?? alternate;
  const originalUrl = resolveUrl(alternate, feedUrl);
  const canonicalUrl = resolveUrl(canonical, feedUrl) ?? originalUrl;

  if (originalUrl === undefined || canonicalUrl === undefined) {
    return undefined;
  }

  const sourceItemId = boundedText(optionalText(entry.id) ?? canonicalUrl, canonicalUrl);
  const title = boundedText(optionalText(entry.title), canonicalUrl);
  const publishedAt = parseOptionalDate(optionalText(entry.published) ?? optionalText(entry.updated));
  const excerpt = excerptText(optionalText(entry.summary) ?? optionalText(entry.content));
  const imageUrl = imageHint(entry, feedUrl);
  const language = attr(entry, "lang") ?? feedLanguage;

  return {
    sourceItemId,
    originalUrl,
    canonicalUrl,
    title,
    sourceName,
    ...(publishedAt === undefined ? {} : {
      publishedAt
    }),
    ...(excerpt === undefined ? {} : {
      excerpt
    }),
    ...(imageUrl === undefined ? {} : {
      imageUrl
    }),
    ...(language === undefined ? {} : {
      language
    })
  };
}

type XmlRecord = Readonly<Record<string, unknown>>;

function asRecord(value: unknown): XmlRecord | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as XmlRecord
    : undefined;
}

function firstRecord(value: unknown): XmlRecord | undefined {
  return records(value)[0];
}

function records(value: unknown): readonly XmlRecord[] {
  if (Array.isArray(value)) {
    return value.flatMap((entry) => {
      const record = asRecord(entry);

      return record === undefined ? [] : [
        record
      ];
    });
  }

  const record = asRecord(value);

  return record === undefined ? [] : [
    record
  ];
}

function optionalText(value: unknown): string | undefined {
  const text = textValue(value);

  return text.length === 0 ? undefined : text;
}

function textValue(value: unknown): string {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return cleanText(String(value));
  }

  if (Array.isArray(value)) {
    return value.map(textValue).find((entry) => entry.length > 0) ?? "";
  }

  const record = asRecord(value);

  if (record === undefined) {
    return "";
  }

  return optionalText(record["#cdata"]) ?? optionalText(record["#text"]) ?? "";
}

function cleanText(value: string): string {
  let output = "";
  let pendingSpace = false;

  for (const char of value) {
    if (isWhitespace(char)) {
      pendingSpace = output.length > 0;
      continue;
    }

    if (pendingSpace && !isPunctuation(char)) {
      output += " ";
    }

    output += char;
    pendingSpace = false;
  }

  return output.trim();
}

function boundedText(value: string | undefined, fallback: string): string {
  const text = cleanText(value ?? fallback);

  return text.slice(0, MAX_TEXT_LENGTH);
}

function excerptText(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  const text = cleanText(stripXmlTags(value));

  return text.length === 0 ? undefined : text.slice(0, MAX_EXCERPT_LENGTH);
}

function stripXmlTags(value: string): string {
  let output = "";
  let insideTag = false;

  for (const char of value) {
    if (char === "<") {
      insideTag = true;
      output += " ";
      continue;
    }

    if (char === ">") {
      insideTag = false;
      output += " ";
      continue;
    }

    if (!insideTag) {
      output += char;
    }
  }

  return output;
}

function isWhitespace(char: string): boolean {
  return char === " " ||
    char === "\n" ||
    char === "\r" ||
    char === "\t" ||
    char === "\f" ||
    char === "\v" ||
    char === "\u00a0";
}

function isPunctuation(char: string): boolean {
  return char === "," ||
    char === "." ||
    char === ";" ||
    char === ":" ||
    char === "!" ||
    char === "?";
}

function resolveUrl(value: string | undefined, baseUrl: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  try {
    const url = new URL(value, baseUrl);
    url.hash = "";

    return url.toString();
  } catch {
    return undefined;
  }
}

function attr(record: XmlRecord, name: string): string | undefined {
  return optionalText(record[`@${name}`]);
}

function atomLink(value: unknown, rel: string | undefined): string | undefined {
  for (const link of records(value)) {
    const linkRel = attr(link, "rel") ?? "alternate";

    if (rel !== undefined && linkRel !== rel) {
      continue;
    }

    const href = attr(link, "href") ?? optionalText(link);

    if (href !== undefined) {
      return href;
    }
  }

  return undefined;
}

function imageHint(item: XmlRecord, feedUrl: string): string | undefined {
  const enclosure = firstRecord(item.enclosure);
  const enclosureType = enclosure === undefined ? undefined : attr(enclosure, "type");
  const enclosureUrl = enclosure === undefined ? undefined : attr(enclosure, "url");

  if (enclosureUrl !== undefined && (enclosureType === undefined || enclosureType.startsWith("image/"))) {
    return resolveUrl(enclosureUrl, feedUrl);
  }

  const thumbnail = firstRecord(item.thumbnail);
  const thumbnailUrl = thumbnail === undefined ? undefined : attr(thumbnail, "url");

  return resolveUrl(thumbnailUrl, feedUrl);
}

function parseOptionalDate(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  const date = new Date(value);
  const time = date.getTime();

  return Number.isNaN(time) ? undefined : date.toISOString();
}
