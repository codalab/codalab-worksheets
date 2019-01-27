import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { CODALAB_VERSION } from '../constants';

class Footer extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return (
            <footer className='navbar-fixed-bottom'>
                <div className='container-fluid'>
                    <ul className='nav navbar-nav navbar-right'>
                        <li>
                            <a
                                href='https://github.com/codalab/codalab-worksheets/wiki/Project_About_CodaLab'
                                target='_blank'
                            >
                                About
                            </a>
                        </li>
                        <li>
                            <a
                                href='https://github.com/codalab/codalab-worksheets/wiki/Privacy'
                                target='_blank'
                            >
                                Privacy and Terms
                            </a>
                        </li>
                        <li>
                            <a
                                href='https://github.com/codalab/codalab-worksheets/wiki/Worksheet-Features'
                                target='_blank'
                            >
                                v{CODALAB_VERSION}
                            </a>
                        </li>
                    </ul>
                </div>
            </footer>
        );
    }
}

export default Footer;
