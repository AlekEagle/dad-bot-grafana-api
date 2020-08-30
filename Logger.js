let chalk = require("chalk");
let _console = { ...console };
let date = function getNowDateTimeStr() {
  let now = new Date();
  let hour = now.getHours();
  return [
    [
      AddZero(now.getDate()),
      AddZero(now.getMonth() + 1),
      now.getFullYear(),
    ].join("/"),
    [AddZero(hour), AddZero(now.getMinutes()), AddZero(now.getSeconds())].join(
      ":"
    ),
  ].join(" ");
};

function AddZero(num) {
  return num >= 0 && num < 10 ? "0" + num : num + "";
}

class Logger {
  log(thing, ...args) {
    _console.log(
      `${chalk.blue.inverse(date())} ${chalk.inverse(`[LOG]`)} ${chalk.white(
        thing
      )}`,
      !args.length ? " " : args
    );
  }
  warn(thing, ...args) {
    _console.log(
      `${chalk.blue.inverse(date())} ${chalk
        .rgb(255, 124, 43)
        .inverse(`[WARN]`)} ${chalk.white(thing)}`,
      !args.length ? " " : args
    );
  }
  debug(thing, ...args) {
    _console.log(
      `${chalk.blue.inverse(date())} ${chalk.grey.inverse(
        `[DEBUG]`
      )} ${chalk.white(thing)}`,
      !args.length ? " " : args
    );
  }
  info(thing, ...args) {
    _console.log(
      `${chalk.blue.inverse(date())} ${chalk
        .rgb(255, 255, 0)
        .inverse(`[INFO]`)} ${chalk.white(thing)}`,
      !args.length ? " " : args
    );
  }
  error(thing, ...args) {
    _console.log(
      `${chalk.blue.inverse(date())} ${chalk.bgRgb(
        255,
        0,
        0
      )(`[ERROR]`)} ${chalk.white(thing)}`,
      !args.length ? " " : args
    );
  }
} Logger
module.exports = () => global.console = new Logger();