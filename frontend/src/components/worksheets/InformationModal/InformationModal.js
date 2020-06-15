import * as React from 'react';
import { Modal } from 'react-bootstrap';
import './InformationModal.scss';

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
                        <td>Move cursor to end of worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>g g</kbd>
                        </td>
                        <td>Move cursor to beginning of worksheet</td>
                    </tr>

                    <th>Markdown</th>
                    <tr>
                        <td>
                            <kbd>a t</kbd>
                        </td>
                        <td>Add markdown right below cursor</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>enter</kbd>
                        </td>
                        <td>Start editing markdown</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>ctrl/cmd+enter</kbd>
                        </td>
                        <td>Save edits to markdown</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>esc</kbd>
                        </td>
                        <td>Stop editing markdown without saving</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>backspace/del</kbd>
                        </td>
                        <td>Delete current markdown</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+e</kbd>
                        </td>
                        <td>Edit full worksheet as markdown</td>
                    </tr>

                    <th>Bundles</th>
                    <tr>
                        <td>
                            <kbd>a u</kbd>
                        </td>
                        <td>Upload bundle right below cursor</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a r</kbd>
                        </td>
                        <td>Add a new run bundle right below cursor</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a n</kbd>
                        </td>
                        <td>Rerun current bundle right below cursor</td>
                    </tr>

                    <tr>
                        <td>
                            <kbd>enter</kbd>
                        </td>
                        <td>Toggle current bundle details</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+enter</kbd>
                        </td>
                        <td>Show current bundle in new window</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a s</kbd>
                        </td>
                        <td>Download current bundle</td>
                    </tr>

                    <tr>
                        <td>
                            <kbd>x</kbd>
                        </td>
                        <td>Select current bundle</td>
                    </tr>

                    <tr>
                        <td>
                            <kbd>backspace/del</kbd>
                        </td>
                        <td>Remove selected bundles</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a k</kbd>
                        </td>
                        <td>Kill selected bundles</td>
                    </tr>

                    <tr>
                        <td>
                            <kbd>a c</kbd>
                        </td>
                        <td>Copy selected bundles</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a d</kbd>
                        </td>
                        <td>Cut selected bundles</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a v</kbd>
                        </td>
                        <td>Paste cut/copied bundle(s) right below cursor</td>
                    </tr>

                    <th>Other</th>
                    <tr>
                        <td>
                            <kbd>a f</kbd>
                        </td>
                        <td>Focus on search bar</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+c</kbd>
                        </td>
                        <td>Show/hide web terminal</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+r</kbd>
                        </td>
                        <td>Refresh worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>?</kbd>
                        </td>
                        <td>Show keyboard shortcuts help (this page)</td>
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

let InformationModal = ({ showInformationModal, toggleInformationModal }) => (
    <React.Fragment>
        <div id='update_progress' className='progress-message'>
            <img src='/img/Preloader_Small.gif' /> Updating...
        </div>
        <KeyboardShortcutModal show={showInformationModal} toggle={toggleInformationModal} />
    </React.Fragment>
);

export default InformationModal;
