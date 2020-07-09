import * as React from 'react';
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
                                href='https://codalab-worksheets.readthedocs.io/en/latest/About'
                                target='_blank'
                                rel='noopener noreferrer'
                            >
                                About
                            </a>
                        </li>
                        <li>
                            <a
                                href='https://codalab-worksheets.readthedocs.io/en/latest/Privacy'
                                target='_blank'
                                rel='noopener noreferrer'
                            >
                                Privacy and Terms
                            </a>
                        </li>
                        <li>
                            <a
                                href='https://github.com/codalab/codalab-worksheets/releases'
                                target='_blank'
                                rel='noopener noreferrer'
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
