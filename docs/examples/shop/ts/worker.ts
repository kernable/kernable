// The worker: runs the shop's commands.
import { connect } from "./connect.ts";
import { customer, order } from "./domain.ts";

// #region worker
const kernable = connect();
await kernable.worker(customer, order).run();
// #endregion worker
