import * as React from 'react';
import { Alert, Modal } from 'react-bootstrap';
import './ExtraWorksheetHTML.scss';

const KeyboardShortcutModal = ({ show, toggle }) => (
    <Modal id='glossaryModal' tabIndex='-1' keyboard show={show} onHide={toggle}>
        <Modal.Header closeButton>
            <h4>Keyboard Shortcuts</h4>
        </Modal.Header>
        <Modal.Body>
            <table className='table table-striped'>
                <tbody>
                    <th>Navigation</th>
                    <tr>
                        <td>
                            <kbd>k</kbd> or <kbd>↑</kbd>
                        </td>
                        <td>Move cursor up</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>j</kbd> or <kbd>↓</kbd>
                        </td>
                        <td>Move cursor down</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+g</kbd>
                        </td>
                        <td>Move to end of worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>g g</kbd>
                        </td>
                        <td>Move to beginning of worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>enter</kbd>
                        </td>
                        <td>
                            Open current bundle detail or worksheet (shift+enter: open in new
                            window)
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>x</kbd>
                        </td>
                        <td>Select the bundle row</td>
                    </tr>
                    <th>Editing</th>
                    <tr>
                        <td>
                            <kbd>shift+e</kbd>
                        </td>
                        <td>Edit worksheet in source mode</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>ctrl/cmd+enter</kbd>
                        </td>
                        <td>Save current edit changes in worksheet source/text block</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>backspace/del</kbd>
                        </td>
                        <td>Delete focused items (bundle rows need to be selected)</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>esc</kbd>
                        </td>
                        <td>Exit editing worksheet source/text block</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a t</kbd>
                        </td>
                        <td>Add a cell right below the current focus</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a u</kbd>
                        </td>
                        <td>Upload a file right below the current focus</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a r</kbd>
                        </td>
                        <td>Add a new run</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a n</kbd>
                        </td>
                        <td>Edit and add a rerun in bundle details</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a v</kbd>
                        </td>
                        <td>Paste clipboard content to source after the current line</td>
                    </tr>
                    <th>Bundles operation</th>
                    <tr>
                        <td>
                            <kbd>a d</kbd>
                        </td>
                        <td>Detach all selected bundles from this worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a k</kbd>
                        </td>
                        <td>Kill all selected bundles</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a c</kbd>
                        </td>
                        <td>Copy all selected bundles' ids</td>
                    </tr>
                    <th>Other</th>
                    <tr>
                        <td>
                            <kbd>shift+c</kbd>
                        </td>
                        <td>Show/hide web terminal</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>c c</kbd>
                        </td>
                        <td>Open full web terminal regardless of show/hide status</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+r</kbd>
                        </td>
                        <td>Refresh worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a s</kbd>
                        </td>
                        <td>Download the focused bundle (should the bundle be downloadable)</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>i</kbd>
                        </td>
                        <td>Input the focused worksheet uid to the terminal</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>?</kbd>
                        </td>
                        <td>Show keyboard shortcut help</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>+</kbd>
                        </td>
                        <td>Expand/shrink worksheet size</td>
                    </tr>
                </tbody>
            </table>
            <div>
                For terminal keyboard shortcuts, see{' '}
                <a href='http://terminal.jcubic.pl/api_reference.php#shortcuts' target='_blank'>
                    here
                </a>{' '}
            </div>
        </Modal.Body>
    </Modal>
);

let ExtraWorksheetHTML = ({
    showGlossaryModal,
    toggleGlossaryModal,
    errorMessage,
    clearErrorMessage,
}) => (
    <React.Fragment>
        <div id='update_progress' className='progress-message'>
            <img src='/img/Preloader_Small.gif' /> Updating...
        </div>
        {/* TODO: Move all error messages to worksheet dialog */}
        {errorMessage && (
            <Alert className='codalab-error-message' bsStyle='danger' onDismiss={clearErrorMessage}>
                <i className='glyphicon glyphicon-remove-circle' /> Error: {errorMessage}
            </Alert>
        )}
        <KeyboardShortcutModal show={showGlossaryModal} toggle={toggleGlossaryModal} />
    </React.Fragment>
);

export default ExtraWorksheetHTML;
