const chalk = require('chalk');
const log = require('loglevel');
const prefix = require('loglevel-plugin-prefix');

const colors = {
  TRACE: chalk.magenta,
  DEBUG: chalk.cyan,
  INFO: chalk.green,
  WARN: chalk.yellow,
  ERROR: chalk.red,
};

prefix.reg(log);
log.enableAll();

prefix.apply(log, {
  format(level) {
    const timestamp = new Date().toISOString();

    return `${chalk.gray(`${timestamp}`)} ${colors[level.toUpperCase()](level)}`;
  },
});

module.exports = {log}