const FRONTEND_ARGS = ["CODALAB_RECAPTCHA_SITE_KEY"];

fs = require("fs");
vars = FRONTEND_ARGS.map(
  (arg) => `window.env.REACT_APP_${arg}="${process.env[arg]}"`
);

fs.writeFileSync("build/js/env.js", "window.env = {}\n" + vars.join("\n"));
