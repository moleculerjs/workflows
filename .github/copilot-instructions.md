# COPILOT EDITS OPERATIONAL GUIDELINES

This a Moleculer middleware project written in Typescript. It is designed to add workflow capabilities (like Temporal.io or Restate.dev) to Moleculer services. 
The project uses adapter-pattern. Currently only Redis adapter is implemented, but it is designed to be extensible to other adapters in the future.

## CODE STANDARDS

### CODING STYLE
- Use TypeScript for all code.
- Follow the TypeScript coding standards and best practices.
- Use descriptive variable and function names.
- Write clear and concise comments where necessary.
- Write integration tests for all features. Unit tests only for critical parts of the code.
- Use consistent tab indentation and double quotes.
- Use arrow functions for anonymous functions.
- Always write JSDoc for every methods and classes.
- If the Middleware options, Workflow options, or Adapter options are changed, update the README.md file accordingly.

## DEVELOPMENT WORKFLOW

- Install dependencies with `npm install`.
- Build: `npm run build`.
- Run tests: `npm run test`.
- Lint code: `npm run lint`.
- Check types: `npm run check`.

## LARGE FILE & COMPLEX CHANGE PROTOCOL

### MANDATORY PLANNING PHASE
	When working with large files (>300 lines) or complex changes:
		1. ALWAYS start by creating a detailed plan BEFORE making any edits
        2. Your plan MUST include:
                - All functions/sections that need modification
                - The order in which changes should be applied
                - Dependencies between changes
                - Estimated number of separate edits required
            
        3. Format your plan as:
            ## PROPOSED EDIT PLAN
                Working with: [filename]
                Total planned edits: [number]

### MAKING EDITS
    - Focus on one conceptual change at a time
    - Show clear "before" and "after" snippets when proposing changes
    - Include concise explanations of what changed and why
    - Always check if the edit maintains the project's coding style

### REFACTORING GUIDANCE
    When refactoring large files:
    - Break work into logical, independently functional chunks
    - Ensure each intermediate state maintains functionality
    - Consider temporary duplication as a valid interim step
    - Always indicate the refactoring pattern being applied
                
### RATE LIMIT AVOIDANCE
    - For very large files, suggest splitting changes across multiple sessions
    - Prioritize changes that are logically complete units
    - Always provide clear stopping points

## Repository Structure

- `dist/`: Contains the compiled JavaScript code.
- `examples/`: Contains example projects demonstrating how to use the library.
- `src/`: Contains the source code of the library.
    - `adapters/`: Contains the source code of the adapters.
        - `base.ts`: Abstract base adapter class.
        - `redis.ts`: Redis adapter implementation.
    - `constants.ts`: Constants values.
    - `errors.ts`: Contains custom error classes.
    - `middleware.ts`: Middleware source code.
    - `moleculer-types.ts`: Augmentation of Moleculer types.
    - `tracing.ts`: Tracing middleware for Workflows middleware.
    - `types.ts`: Typescript types.
    - `utils.ts`: Utility functions.
    - `workflow.ts`: Workflow class source code. It contains the main logic for managing workflows.
- `test/`: Contains tests.
    - `integration/`: Contains integration tests (Vitest).
    - `unit/`: Contains integration tests (Vitest).
    - `docker-compose.yml`: Docker-Compose to run mandatory services for integration tests.

### Before commit

- Run `npm run lint` to check for linting errors.
- Run `npm run check` to perform type checking.
- Run `npm run test` to ensure all tests pass.
- Run `npm run build` to compile the TypeScript code to JavaScript.
