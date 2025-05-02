module.exports = {
	env: {
		node: true,
		commonjs: true,
		es6: true,
		jquery: false,
		jest: true,
		jasmine: true
	},
	extends: ["eslint:recommended", "plugin:prettier/recommended"],
	parserOptions: {
		sourceType: "module",
		ecmaVersion: 2020
	},
	plugins: ["node", "promise"],
	rules: {
		"no-var": ["error"],
		"no-console": ["warn"],
		"no-unused-vars": ["warn"],
		"no-trailing-spaces": ["error"],
		"no-process-exit": ["off"],
		"node/no-unpublished-require": 0,
		"require-atomic-updates": 0,
		"object-curly-spacing": ["warn", "always"]
	}
};
