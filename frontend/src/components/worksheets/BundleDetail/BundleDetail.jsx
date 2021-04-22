// @flow
import * as React from 'react';
import * as $ from 'jquery';
// import Drawer from '@material-ui/core/Drawer';
import { JsonApiDataStore } from 'jsonapi-datastore';

import ConfigurationPanel from '../ConfigPanel';
import MainContent from './MainContent';
import BundleDetailSideBar from './BundleDetailSideBar';
import BundleActions from './BundleActions';
import { findDOMNode } from 'react-dom';
import { apiWrapper, fetchFileSummary } from '../../../util/apiWrapper';

class BundleDetail extends React.Component<
    {
        uuid: string,
        // Callback on metadata change.
        bundleMetadataChanged: () => void,
        onClose: () => void,
        onOpen: () => void,
    },
    {
        errorMessages: string[],
        bundleInfo: {},
        fileContents: string,
        stdout: string,
        stderr: string,
    },
> {
    static getDerivedStateFromProps(props, state) {
        // Any time the current bundle uuid changes,
        // clear the error messages and not the actual contents, so that in
        // the side panel, the page doesn't flicker.
        if (props.uuid !== state.prevUuid) {
            return {
                prevUuid: props.uuid,
                errorMessages: [],
            };
        }
        return null;
    }

    constructor(props) {
        super(props);
        this.state = {
            errorMessages: [],
            bundleInfo: null,
            fileContents: null,
            stdout: null,
            stderr: null,
            prevUuid: props.uuid,
            open: true,
            fetch: true,
        };
    }

    componentDidMount() {
        this.fetchBundleMetaData();
        this.fetchBundleContents();
        this.timer = setInterval(() => {
            if (!this.state.bundleInfo) {
                // If info is not available yet, fetch
                this.fetchBundleMetaData();
                this.fetchBundleContents();
                return;
            }
            if (
                this.state.bundleInfo.state.match(
                    'uploading|created|staged|making|starting|preparing|running|finalizing|worker_offline',
                )
            ) {
                // If bundle is in a state that is possible to transition to a different state, fetch data
                // we have ignored ready|failed|killed states here
                this.fetchBundleMetaData();
                this.fetchBundleContents();
            } else {
                // otherwise just clear the timer
                clearInterval(this.timer);
            }
        }, 4000);
        if (this.props.onOpen) {
            this.props.onOpen();
        }
    }

    componentWillUnmount() {
        // Clear the timer
        clearInterval(this.timer);
    }

    fetchBundleMetaData = () => {
        const { uuid } = this.props;
        const callback = (response) => {
            // Normalize JSON API doc into simpler object
            const bundleInfo = new JsonApiDataStore().sync(response);
            bundleInfo.editableMetadataFields = response.data.meta.editable_metadata_keys;
            bundleInfo.metadataType = response.data.meta.metadata_type;
            this.setState({ bundleInfo });
        };
        const errorHandler = (error) => {
            this.setState({
                bundleInfo: null,
                fileContents: null,
                stdout: null,
                stderr: null,
                errorMessages: this.state.errorMessages.concat([error]),
            });
        };
        apiWrapper
            .fetchBundleMetadata(uuid)
            .then(callback)
            .catch(errorHandler);
    };

    // Fetch bundle contents
    fetchBundleContents = () => {
        const { uuid } = this.props;
        const callback = async (response) => {
            const info = response.data;
            if (!info) return;
            if (info.type === 'file' || info.type === 'link') {
                return fetchFileSummary(uuid, '/').then((blob) => {
                    this.setState({ fileContents: blob, stdout: null, stderr: null });
                });
            } else if (info.type === 'directory') {
                // Get stdout/stderr (important to set things to null).
                let fetchRequests = [];
                let stateUpdate = {
                    fileContents: null,
                };
                ['stdout', 'stderr'].forEach(
                    function(name) {
                        if (info.contents.some((entry) => entry.name === name)) {
                            fetchRequests.push(
                                fetchFileSummary(uuid, '/' + name).then((blob) => {
                                    stateUpdate[name] = blob;
                                }),
                            );
                        } else {
                            stateUpdate[name] = null;
                        }
                    }.bind(this),
                );
                await Promise.all(fetchRequests);
                this.setState(stateUpdate);
            }
        };

        const errorHandler = (error) => {
            this.setState({
                bundleInfo: null,
                fileContents: null,
                stdout: null,
                stderr: null,
                errorMessages: this.state.errorMessages.concat(error),
            });
        };

        apiWrapper
            .fetchBundleContents(this.props.uuid)
            .then(callback)
            .catch(errorHandler);
    };

    render(): React.Node {
        const {
            bundleMetadataChanged,
            onUpdate,
            rerunItem,
            showNewRerun,
            showDetail,
            handleDetailClick,
            editPermission,
        } = this.props;
        const { bundleInfo, stdout, stderr, fileContents } = this.state;
        if (!bundleInfo) {
            return <div></div>;
        }
        if (bundleInfo.bundle_type === 'private') {
            return <div>Detail not available for this bundle</div>;
        }

        return (
            <ConfigurationPanel
                //  The ref is created only once, and that this is the only way to properly create the ref before componentDidMount().
                ref={(node) => this.scrollToNewlyOpenedDetail(node)}
                buttons={
                    <BundleActions
                        showNewRerun={showNewRerun}
                        showDetail={showDetail}
                        handleDetailClick={handleDetailClick}
                        bundleInfo={bundleInfo}
                        rerunItem={rerunItem}
                        onComplete={bundleMetadataChanged}
                        editPermission={editPermission}
                    />
                }
                sidebar={
                    <BundleDetailSideBar
                        bundleInfo={bundleInfo}
                        onUpdate={onUpdate}
                        onMetaDataChange={this.fetchBundleMetaData}
                    />
                }
            >
                <MainContent
                    bundleInfo={bundleInfo}
                    stdout={stdout}
                    stderr={stderr}
                    fileContents={fileContents}
                />
            </ConfigurationPanel>
        );
    }
    scrollToNewlyOpenedDetail(node) {
        // Only scroll to the bundle detail when it is opened
        if (node && this.state.open) {
            findDOMNode(node).scrollIntoView({ block: 'center' });
            // Avoid undesirable scroll
            this.setState({ open: false });
        }
    }
}

export default BundleDetail;
