import * as React from 'react';
import Immutable from 'seamless-immutable';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';

/**
 * This stateful component ___.
 */
class UserInfo extends React.Component {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            bundleInfo: null,
            uuid: this.props.match.params['uuid'],
        });
    }

    /** Renderer. */
    render() {
        let renderArray = [];
        renderArray.push(<SubHeader title='' />);
        renderArray.push(
            <ContentWrapper>
                <div>{this.state.uuid}</div>
            </ContentWrapper>,
        );
        return renderArray;
    }
}

export default UserInfo;
