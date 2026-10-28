// The worker: runs the shop's commands.
import { Kernable } from "@kernable/sdk";

import { customerAggregate, orderAggregate } from "./domain.ts";

// #region worker
const kernable = new Kernable();
await kernable.worker(customerAggregate, orderAggregate).run();
// #endregion worker
