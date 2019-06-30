import React from 'react';
import queryString from 'query-string';

export default class extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            loading: true,
            error: false
        };
    }
    async componentDidMount() {
        const { name } = queryString.parse(this.props.location.search);
        try {
            const response = await fetch(`/rest/worksheets?specs=${name}`).then(e => e.json());
            const uuid = response.data[0].id;
            this.props.history.push(`/worksheets/${uuid}/`);
        }
        catch (e) {
            console.error(e);
            this.setState({ error: true, loading: false });
        }
    }
    render() {
        const styles = {
            position: "absolute",
            top: "50%",
            left: "50%",
        }
        return <div>
            {this.state.loading && <div style={styles}><img src={`${process.env.PUBLIC_URL}/img/Preloader_Small.gif`} /></div>}
            {this.state.error && <div>Error.</div>}
        </div>
    }
}
