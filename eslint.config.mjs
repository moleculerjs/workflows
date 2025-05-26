// @ts-check

import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import eslintPluginPrettierRecommended from "eslint-plugin-prettier/recommended";

export default tseslint.config(
	eslint.configs.recommended,
	tseslint.configs.recommended,
	eslintPluginPrettierRecommended,
	{
		rules: {
			"@/no-var": ["error"],
			"@/no-console": ["warn"],
			"@typescript-eslint/no-unused-vars": ["warn"],
			"@/no-trailing-spaces": ["error"],
			"@/no-process-exit": ["off"],
			"@/object-curly-spacing": ["warn", "always"]
		}
	}
);
