/**
 * Replaces all the '/' characters in a directory string with ' ❯ '.
 * @param {string} directory - e.g. folder-1/folder-2/folder-3
 */
export function formatDirectory(directory) {
    if (directory.startsWith('/')) {
        directory = directory.substring(1);
    }
    return directory.replace(/\//g, ' ❯ ');
}
