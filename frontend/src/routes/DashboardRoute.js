import * as React from 'react';
import NewDahboard from '../components/Dashboard/NewDashboard';

/**
 * This route page displays a dashboard specified by the user id.
 */
class DashboardRoute extends React.Component<{
    auth: {
        isAuthenticated: boolean,
    },
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        const { uid } = this.props.match.params;
        this.state = {
            uid,
        };
    }

    /** Renderer. */
    render() {
        const { uid } = this.props.match.params;
        return <NewDahboard uid={uid} auth={this.props.auth} />;
    }
}

export default DashboardRoute;
