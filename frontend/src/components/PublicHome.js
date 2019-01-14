import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';

/**
 * This [pure / stateful] component ___.
 */
const PublicHome = () => {
    return (
        <div>
            <div class='jumbotron'>
                <div class='container'>
                    <div class='row'>
                        <div class='col-sm-12 col-md-8 col-md-offset-2'>
                            <img
                                src={
                                    process.env.PUBLIC_URL +
                                    '/img/codalab-logo-onecolor-reverse.png'
                                }
                                alt='CodaLab'
                                class='img-responsive'
                            />
                            <h4>
                                <b>
                                    <i>A collaborative platform for reproducible research.</i>
                                </b>
                            </h4>
                            <div class='frontpage-buttons'>
                                <span class='user-authenticated frontpage-button'>
                                    <a href='/rest/worksheets/?name=%2F'>My Home</a>
                                </span>
                                <span class='user-authenticated frontpage-button'>
                                    <a href='/rest/worksheets/?name=dashboard'>My Dashboard</a>
                                </span>
                                <span class='user-not-authenticated frontpage-button'>
                                    <a href='/account/signup'>Sign Up</a>
                                </span>
                                <span class='user-not-authenticated frontpage-button'>
                                    <a href='/account/login'>Sign In</a>
                                </span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default PublicHome;
