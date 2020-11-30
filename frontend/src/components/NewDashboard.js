import * as React from 'react';

/**
 * This route page displays the new Dashboard, which is the landing page for all the users.
 */
class NewDashboard extends React.Component<{
    classes: {},
    auth: {
        isAuthenticated: boolean,
        signout: () => void,
    },
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        const { uuid } = this.props.match.params;
        const { auth } = this.props;
        if (!auth.isAuthenticated) {
            this.props.history.push('/account/login');
        }
        this.state = {
            bundleInfo: null,
            uuid,
        };
    }

    /** Renderer. */
    render() {
        return <div>Empty New Dashboard!</div>;
    }
}

export default NewDashboard;
