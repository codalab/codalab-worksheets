// @flow
import * as React from 'react';
import SubHeader from '../SubHeader';
import ContentWrapper from '../ContentWrapper';
import { renderFormat } from '../../util/worksheet_utils';
import './Store.scss';
import ErrorMessage from '../worksheets/ErrorMessage';
import { fetchStores } from '../../util/apiWrapper';

class Store extends React.Component {
    state = {
        errorMessages: [],
        storeInfo: null,
    };
    /**
     * Fetch store data and update the state of this component.
     */
    refreshStore = () => {
        const { uuid } = this.props;
        fetchStores(uuid)
            .then((response) => {
                const {
                    data: { attributes: storeInfo },
                } = response;
                console.log('storeInfo', storeInfo);

                this.setState({
                    storeInfo,
                });
            })
            .catch((err) => {
                this.setState({ errorMessages: [err.toString()] });
            });
    };

    componentDidMount = () => {
        this.refreshStore();
    };

    /** Renderer. */
    render = () => {
        const storeInfo = this.state.storeInfo;
        console.log('storeInfo', storeInfo);
        if (!storeInfo) {
            // Error
            if (this.state.errorMessages.length > 0) {
                return <ErrorMessage message={"Not found: '/stores/" + this.props.uuid + "'"} />;
            }

            // Still loading
            return (
                <div id='store-message' className='store-detail'>
                    <img alt='Loading' src={`${process.env.PUBLIC_URL}/img/Preloader_Small.gif`} />{' '}
                    Loading store info...
                </div>
            );
        }

        const storeMetadataChanged = this.refreshStore;

        const content = (
            <div id='panel_content'>
                {renderErrorMessages(this.state.errorMessages)}
                {renderHeader(storeInfo, storeMetadataChanged)}
            </div>
        );
        return (
            <div id='store-content'>
                <React.Fragment>
                    <SubHeader title='Store View' />
                    <ContentWrapper>{content}</ContentWrapper>
                </React.Fragment>
            </div>
        );
    };
}

function renderErrorMessages(messages) {
    return (
        <div id='store-error-messages'>
            {messages.map((message) => {
                return <div className='alert alert-danger alert-dismissable'>{message}</div>;
            })}
        </div>
    );
}

function createRow(key, value) {
    return (
        <tr key={key}>
            <th>
                <span>{key}</span>
            </th>
            <td>
                <span>{value}</span>
            </td>
        </tr>
    );
}

function renderHeader(storeInfo) {
    // Display basic information.
    let rows = [];
    rows.push(createRow('name', storeInfo.name));
    rows.push(createRow('uuid', storeInfo.uuid));
    rows.push(createRow('bundle store type', storeInfo.storage_type));
    rows.push(createRow('owner', storeInfo.owner === null ? '<anonymous>' : storeInfo.owner));
    rows.push(createRow('location', storeInfo.url));

    return (
        <div>
            <table className='store-meta table'>
                <tbody>{rows.map((elem) => elem)}</tbody>
            </table>
        </div>
    );
}

export default Store;
