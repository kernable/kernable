// How every process of the shop connects: from the environment (part 8), or to the local kernel
// of part 1.
import { Kernable, type Options } from "@kernable/sdk";

// #region connect
export function connect(options: Partial<Options> = {}): Kernable {
  const env = process.env;
  const tls = env.KERNABLE_TLS_CA ? { ca: env.KERNABLE_TLS_CA, cert: env.KERNABLE_TLS_CERT!, key: env.KERNABLE_TLS_KEY! } : undefined;
  return new Kernable({ url: env.KERNABLE_URL ?? `unix:${process.cwd()}/k.sock`, key: env.KERNABLE_KEY ?? "app.key", tls, ...options });
}
// #endregion connect
