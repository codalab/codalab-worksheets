import * as React from 'react';
import { Modal } from 'react-bootstrap';
import './ExtraWorksheetHTML.scss';

const GlossaryModal = ({ show, toggle }) => (
    <Modal id='glossaryModal' tabIndex='-1' keyboard show={show} onHide={toggle}>
        <Modal.Header closeButton>
            <h4>Keyboard Shortcuts</h4>
        </Modal.Header>
        <Modal.Body>
            <a href='http://terminal.jcubic.pl/api_reference.php' target='_blank'>
                {' '}
                More shortcuts for web terminal (see Keyboard shortcuts section){' '}
            </a>
            <table className='table table-striped'>
                <tbody>
                    <tr>
                        <td>
                            <kbd>k</kbd> or <kbd>↑</kbd>
                        </td>
                        <td>Move cursor up</td>
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
                            <kbd>t</kbd>
                        </td>
                        <td>Add a cell right below the current focus</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>u</kbd>
                        </td>
                        <td>Upload a file right below the current focus</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>a+r</kbd>
                        </td>
                        <td>Add a new run</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>e</kbd>
                        </td>
                        <td>Edit entire worksheet in Markdown</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+r</kbd>
                        </td>
                        <td>Refresh worksheet</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>enter</kbd>
                        </td>
                        <td>Open current bundle detail or worksheet (shift+enter: open in new window)</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>ctrl/command+enter</kbd>
                        </td>
                        <td>Save current edit</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>backspace/del</kbd>
                        </td>
                        <td>Quit edit in markdown / Delete selected bundles</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>esc</kbd>
                        </td>
                        <td>Exit editing</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>shift+c</kbd>
                        </td>
                        <td>Show/hide web terminal</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>c</kbd>
                        </td>
                        <td>Focus on the web terminal regardless of show/hide status</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>j</kbd> or <kbd>↓</kbd>
                        </td>
                        <td>Move cursor down</td>
                    </tr>
                    <tr>
                        <td>
                            <kbd>?</kbd>
                        </td>
                        <td>Show keyboard shortcut help</td>
                    </tr>
                </tbody>
            </table>
        </Modal.Body>
    </Modal>
);

let ExtraWorksheetHTML = ({ showGlossaryModal, toggleGlossaryModal }) => (
    <React.Fragment>
        <div id='update_progress' className='progress-message'>
            <img src='/img/Preloader_Small.gif' /> Updating...
        </div>
        <div id='save_error' className='progress-message'>
            <i className='glyphicon glyphicon-remove-circle' /> Error saving...
        </div>
        <GlossaryModal show={showGlossaryModal} toggle={toggleGlossaryModal} />
    </React.Fragment>
);

export default ExtraWorksheetHTML;
