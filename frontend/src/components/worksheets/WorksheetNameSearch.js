import React from 'react';
import queryString from 'query-string';
import ErrorMessage from './ErrorMessage';
import Loading from '../Loading';

export default class extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            loading: true,
            error: false,
        };
    }
    async componentDidMount() {
        let { name } = queryString.parse(this.props.location.search);
        if (name === undefined) {
            // Redirect to dashboard if logged in else home
            name = this.props.auth.isAuthenticated ? 'dashboard' : 'home';
        }

        try {
            const response = await fetch(`/rest/worksheets?specs=${name}`).then((e) => e.json());
            const uuid = response.data[0].id;
            this.props.history.replace(`/worksheets/${uuid}/`);
        } catch (e) {
            // Error shouldn't happen anymore, keeping just in case
            console.error(e);
            this.setState({ error: true, loading: false });
        }
    }
    render() {
        return (
            <div>
                {this.state.loading && <Loading style={{ marginTop: 30 }} />}
                {this.state.error && (
                    <ErrorMessage message={'Error. Please provide a worksheet uuid'} />
                )}
            </div>
        );
    }
}
