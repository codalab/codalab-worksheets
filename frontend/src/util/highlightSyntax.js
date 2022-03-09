import hljs from 'highlight.js';

/**
 * Serializes an HTML document to a string.
 *
 * @param {object} document
 * @returns {string}
 */
function serialize(document) {
    const serializer = new XMLSerializer();
    return serializer.serializeToString(document);
}

/**
 * Highlights <code> tag contents using highlight.js.
 * highlight.js styles are loaded in <head>.
 *
 * @param {string} text
 * @returns {string}
 */
export function highlightSyntax(text) {
    const parser = new DOMParser();
    const doc = parser.parseFromString(text, 'text/xml');
    const codeTags = doc.getElementsByTagName('code');
    if (!codeTags.length) {
        return text;
    }

    const codeTag = codeTags[0];
    const languageClass = codeTag.className;
    if (!languageClass.includes('language-')) {
        return text;
    }

    const language = languageClass.replace('language-', '');
    const highlightedCode = hljs.highlight(codeTag.innerHTML, { language }).value;
    codeTag.innerHTML = highlightedCode;
    return serialize(doc);
}
