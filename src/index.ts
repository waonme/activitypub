import "./logging.ts";
import { serve } from "@hono/node-server";
import { behindProxy } from "x-forwarded-fetch";
import { getLogger } from "@logtape/logtape";
import app from "./app.ts";
import { startEntityBroker } from "./daemon.ts";
import { config } from "./config.ts";

const logger = getLogger("activitypub");

await startEntityBroker();

// HTTP署名(RFC 9421の@target-uri/@authority等)の検証はリクエストURLのscheme/hostに
// 依存するため、プロキシヘッダの有無に関わらず公開origin(baseUrl)へ強制正規化する
const canonical = new URL(config.activitypub.baseUrl);

const canonicalized = (fetch: (request: Request) => Response | Promise<Response>) =>
  async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    if (url.protocol === canonical.protocol && url.host === canonical.host) {
      return await fetch(request);
    }
    logger.debug(`Canonicalizing request URL: ${request.url} -> origin ${canonical.origin}`);
    url.protocol = canonical.protocol;
    url.host = canonical.host;
    url.port = canonical.port;
    const headers = new Headers(request.headers);
    headers.set("Host", canonical.host);
    return await fetch(new Request(url, {
      method: request.method,
      headers,
      body: request.method === "GET" || request.method === "HEAD"
        ? undefined
        : await request.blob(),
    }));
  };

serve(
  {
    port: config.server.port,
    fetch: behindProxy(canonicalized(app.fetch.bind(app))),
  },
  (info) =>
    logger.info(`Server started at http://${info.address}:${info.port}`),
);
