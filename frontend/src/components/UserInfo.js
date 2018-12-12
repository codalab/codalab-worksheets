import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { serverHost } from '../ServerConstants';
import $ from 'jquery';

/**
 * This [pure / stateful] component ___.
 */
class UserInfo extends React.Component<
    {
        /** React components within opening & closing tags. */
        children: React.Node,
    },
    {
        // Optional: type declaration of this.state.
    },
> {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    processData(response) {
        // Shim in links to change email and password
        var user = response.data;
        user.attributes.email = (
            <span>
                {user.attributes.email} <a href='/account/changeemail'>(change)</a>
            </span>
        );
        user.attributes.password = (
            <span>
                ******** <a href='/rest/account/reset'>(change)</a>
            </span>
        );
        return user;
    }

    componentDidMount() {
        $.ajax({
            method: 'GET',
            url: serverHost + '/rest/user',
            dataType: 'json',
            context: this,
        })
            .done(function(response) {
                console.log(response);
                this.setState({
                    user: this.processData(response),
                });
            })
            .fail(function(xhr, status, err) {
                this.setState({
                    errors: xhr.responseText,
                });
            });
    }

    /** Renderer. */
    render() {
        return <div />;
    }
}

export default UserInfo;
