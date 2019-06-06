// @flow
import * as React from 'react';
import * as $ from 'jquery';
// import Drawer from '@material-ui/core/Drawer';
import { JsonApiDataStore } from 'jsonapi-datastore';

import ConfigurationPanel from '../ConfigPanel';
import MainContent from './MainContent';
import SideBar from './SideBar';
import BundleActions from './BundleActions';

class BundleDetail extends React.Component<
    {
        uuid: string,
        // Callback on metadata change.
        bundleMetadataChanged: () => void,
        onClose: () => void,
    },
    {
        errorMessages: string[],
        bundleInfo: {},
        fileContents: string,
        stdout: string,
        stderr: string,
    }
> {

    static getDerivedStateFromProps(props, state) {
        // Any time the current bundle uuid changes,
        // clear the error messages and not the actual contents, so that in
        // the side panel, the page doesn't flicker.
        if (props.uuid !== state.prevUuid) {
            return {
                prevUuid: props.uuid,
                errorMessages: [],
                open: true,
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
        };
    }

    componentDidMount() {
        this.fetchBundleMetaData();
        this.fetchBundleContents();
    }

    /**
     * Return a Promise to fetch the summary of the given file.
     * @param uuid  uuid of bundle
     * @param path  path within the bundle
     * @return  jQuery Deferred object
     */
    fetchFileSummary(uuid, path) {
        return $.ajax({
            type: 'GET',
            url: '/rest/bundles/' + uuid + '/contents/blob' + path,
            data: {
                head: 50,
                tail: 50,
                truncation_text: '\n... [truncated] ...\n\n',
            },
            dataType: 'text',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
        });
    }

    fetchBundleMetaData = () => {
        const { uuid } = this.props;

        $.ajax({
            type: 'GET',
            url: '/rest/bundles/' + uuid,
            data: {
                include_display_metadata: 1,
                include: 'owner,group_permissions,host_worksheets',
            },
            dataType: 'json',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
        }).then(function(response) {
            // Normalize JSON API doc into simpler object
            const bundleInfo = new JsonApiDataStore().sync(response);
            bundleInfo.editableMetadataFields = response.data.meta.editable_metadata_keys;
            bundleInfo.metadataType = response.data.meta.metadata_type;
            this.setState({ bundleInfo });
        }).fail(function(xhr, status, err) {
            this.setState({
                bundleInfo: null,
                fileContents: null,
                stdout: null,
                stderr: null,
                errorMessages: this.state.errorMessages.concat([xhr.responseText]),
            });
        });
    }

    // Fetch bundle contents
    fetchBundleContents = () => {
        const { uuid } = this.props;

        $.ajax({
            type: 'GET',
            url: '/rest/bundles/' + uuid + '/contents/info/',
            data: {
                depth: 1,
            },
            dataType: 'json',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
        }).then(function(response) {
            const info = response.data;
            if (!info) return;
            if (info.type === 'file' || info.type === 'link') {
                return this.fetchFileSummary(uuid, '/').then(function(blob) {
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
                                this.fetchFileSummary(uuid, '/' + name).then(
                                    function(blob) {
                                        stateUpdate[name] = blob;
                                    },
                                ),
                            );
                        } else {
                            stateUpdate[name] = null;
                        }
                    }.bind(this),
                );
                $.when.apply($, fetchRequests).then(() => {
                    this.setState(stateUpdate);
                });
                return $.when(fetchRequests);
            }
        }).fail(function(xhr, status, err) {
            // 404 Not Found errors are normal if contents aren't available yet, so ignore them
            if (xhr.status != 404) {
                this.setState({
                    bundleInfo: null,
                    fileContents: null,
                    stdout: null,
                    stderr: null,
                    errorMessages: this.state.errorMessages.concat([xhr.responseText]),
                });
            } else {
                // If contents aren't available yet, then also clear stdout and stderr.
                this.setState({ fileContents: null, stdout: null, stderr: null });
            }
        });
    }
  
    render(): React.Node {
        const { uuid, onUpdate, onClose } = this.props;
        const {
            bundleInfo,
            stdout,
            stderr,
            fileContents } = this.state;

        if (!bundleInfo) {
            return null;
        }

        return (
            <ConfigurationPanel
                buttons={ <BundleActions bundleInfo={ bundleInfo } /> }
                sidebar={ <SideBar bundleInfo={ bundleInfo } onUpdate={ onUpdate } onMetaDataChange={ this.fetchBundleMetaData } /> }
            >
                <MainContent
                    bundleInfo={ bundleInfo }
                    stdout={ stdout }
                    stderr={ stderr }
                    fileContents={ fileContents }
                />
            </ConfigurationPanel>
        );
  }
}

export default BundleDetail;
