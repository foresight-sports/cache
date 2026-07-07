export default {
    clearMocks: true,
    moduleFileExtensions: ["js", "ts"],
    // The source uses ESM-style explicit `.js` extensions on relative imports
    // (e.g. `../actionsCacheShims.js`), which resolve to the compiled output in
    // a real build. Under ts-jest those specifiers must be mapped back to the
    // extensionless form so jest resolves the sibling `.ts` via
    // moduleFileExtensions. Without this, any suite that transitively imports
    // such a module fails to load ("Cannot find module '../actionsCacheShims.js'").
    moduleNameMapper: {
        "^(\\.{1,2}/.*)\\.js$": "$1"
    },
    roots: ["<rootDir>/__tests__"],
    testEnvironment: "node",
    testMatch: ["**/*.test.ts"],
    transform: {
        "^.+\\.ts$": [
            "ts-jest",
            {
                useESM: true,
                diagnostics: {
                    ignoreCodes: [151002]
                }
            }
        ]
    },
    extensionsToTreatAsEsm: [".ts"],
    transformIgnorePatterns: ["node_modules/(?!(@actions)/)"],
    verbose: true
};
