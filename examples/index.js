"use strict";

import process from "node:process";

const moduleName = process.argv[2] || "simple";
process.argv.splice(2, 1);

import("./" + moduleName + "/index.js");
