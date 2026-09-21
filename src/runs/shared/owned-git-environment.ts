const LOCAL_GIT_ENVIRONMENT = new Set([
	"GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT",
	"GIT_OBJECT_DIRECTORY", "GIT_DIR", "GIT_WORK_TREE", "GIT_IMPLICIT_WORK_TREE", "GIT_GRAFT_FILE",
	"GIT_INDEX_FILE", "GIT_NO_REPLACE_OBJECTS", "GIT_REPLACE_REF_BASE", "GIT_PREFIX", "GIT_SHALLOW_FILE",
	"GIT_COMMON_DIR", "GIT_INTERNAL_SUPER_PREFIX", "GIT_NAMESPACE",
]);

/** Retain identity and authentication while preventing inherited Git repository routing. */
export function ownedGitEnvironment(source: NodeJS.ProcessEnv) {
	const env: Record<string, string> = {};
	for (const [key, value] of Object.entries(source)) {
		if (value === undefined || LOCAL_GIT_ENVIRONMENT.has(key) || /^GIT_CONFIG_(?:KEY|VALUE)_\d+$/.test(key)) continue;
		env[key] = value;
	}
	return env;
}
