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
        const { name } = queryString.parse(this.props.location.search);
        try {
            const response = await fetch(`/rest/worksheets?specs=${name}`).then((e) => e.json());
            const uuid = response.data[0].id;
            this.props.history.push(`/worksheets/${uuid}/`);
        } catch (e) {
            console.error(e);
            this.setState({ error: true, loading: false });
        }
    }
    render() {
        return (
            <div>
                {this.state.loading && <Loading />}
                {this.state.error && (
                    <ErrorMessage message={'Error. Please provide a worksheet uuid'} />
                )}
            </div>
        );
    }
}
