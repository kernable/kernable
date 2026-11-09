// Registers the fulfilment process with the kernel: once per deploy, after the worker is up.
import { Kernable } from "@kernable/sdk";

import { fulfilment } from "./domain.ts";

// #region register
const kernable = new Kernable();
const { name, version, new: changed } = await kernable.registerProcess(fulfilment);
console.log(`${name} version ${version}${changed ? "" : " (unchanged)"}`);
kernable.close();
// #endregion register
