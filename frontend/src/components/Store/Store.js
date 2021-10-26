// @flow
import * as React from 'react';
import SubHeader from '../SubHeader';
import ContentWrapper from '../ContentWrapper';
import { renderFormat } from '../../util/worksheet_utils';
import './Store.scss';
import ErrorMessage from '../worksheets/ErrorMessage';

class Store extends React.Component {
    state = {
        errorMessages: [],
        storeInfo: null,
    };

    /**
     * Fetch store data and update the state of this component.
     */
    refreshStore = () => {
        // TODO: Implement actual API call.
        this.setState({
            storeInfo: {
                owner: {
                    user_name: 'codalab',
                },
                uuid: this.props.uuid,
                metadataType: {},
            },
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

function createRow(storeInfo, storeMetadataChanged, key, value) {
    // Return a row corresponding to showing
    //   key: value
    // which can be edited.
    let fieldType = storeInfo.metadataType;

    return (
        <tr key={key}>
            <th>
                <span>{key}</span>
            </th>
            <td>
                <span>{renderFormat(value, fieldType[key])}</span>
            </td>
        </tr>
    );
}

function renderHeader(storeInfo, storeMetadataChanged) {
    let storeDownloadUrl = '/rest/stores/' + storeInfo.uuid + '/contents/blob/';

    // Display basic information
    let rows = [];
    rows.push(createRow(storeInfo, storeMetadataChanged, 'uuid', storeInfo.uuid));
    rows.push(createRow(storeInfo, storeMetadataChanged, 'bundle store type', 'PLACEHOLDER'));
    rows.push(
        createRow(
            storeInfo,
            storeMetadataChanged,
            'owner',
            storeInfo.owner === null ? '<anonymous>' : storeInfo.owner.user_name,
        ),
    );
    rows.push(createRow(storeInfo, storeMetadataChanged, 'location', 'PLACEHOLDER'));

    return (
        <div>
            <table className='store-meta table'>
                <tbody>
                    {rows.map(function(elem) {
                        return elem;
                    })}
                </tbody>
            </table>
        </div>
    );
}

export default Store;
