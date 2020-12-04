import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import SideBar from './SideBar';
import MainPanel from './MainPanel';
import $ from 'jquery';

/**
 * This route page displays the new Dashboard, which is the landing page for all the users.
 */
class NewDashboard extends React.Component<{
    classes: {},
    auth: {
        isAuthenticated: boolean,
    },
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        const { auth } = this.props;
        if (!auth.isAuthenticated) {
            // Redirect to Login page if not authenticated
            this.props.history.push('/account/login');
        }
        this.state = {
            userInfo: null, // User info of the current user. (null is the default)
        };
    }

    componentDidMount() {
        $('body').addClass('ws-interface');
        // Fetch the current user's information
        $.ajax({
            url: '/rest/user',
            dataType: 'json',
            cache: false,
            type: 'GET',
            success: function(data) {
                var userInfo = data.data.attributes;
                userInfo.user_id = data.data.id;
                this.setState({
                    userInfo: userInfo,
                });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(xhr.responseText);
            },
        });
    }

    /** Renderer. */
    render() {
        return (
            <div>
                <Grid container spacing={30}>
                    <Grid item xs={3}>
                        <SideBar userInfo={this.state.userInfo}></SideBar>
                    </Grid>
                    <Grid item xs>
                        <MainPanel userInfo={this.state.userInfo}></MainPanel>
                    </Grid>
                </Grid>
            </div>
        );
    }
}

export default NewDashboard;
