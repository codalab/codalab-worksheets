// @flow
import * as React from 'react';
import * as $ from 'jquery';
import Drawer from '@material-ui/core/Drawer';
import { JsonApiDataStore } from 'jsonapi-datastore';

import ConfigurationPanel from '../ConfigurationPanel';
import MainContent from './MainContent';
import SideBar from './SideBar';
import BundleActions from './BundleActions';

const fakestdout = "Lorem ipsum luctus non nulla accumsan eu rhoncus ut velit sagittis aliquet, morbi posuere interdum bibendum sollicitudin nisi nulla vulputate fringilla augue, etiam felis mauris feugiat massa eu leo torquent sapien aliquam luctus semper dapibus inceptos per quis aliquet urna, facilisis fusce dui placerat potenti taciti, rhoncus iaculis imperdiet venenatis sociosqu id suspendisse condimentum nulla vehicula lacinia et laoreet viverra class vehicula, sollicitudin quisque primis habitant nisi aenean placerat vehicula platea commodo ligula arcu aliquam ad.Lorem ipsum luctus non nulla accumsan eu rhoncus ut velit sagittis aliquet, morbi posuere interdum bibendum sollicitudin nisi nulla vulputate fringilla augue, etiam felis mauris feugiat massa eu leo torquent sapien aliquam luctus semper dapibus inceptos per quis aliquet urna, facilisis fusce dui placerat potenti taciti, rhoncus iaculis imperdiet venenatis sociosqu id suspendisse condimentum nulla vehicula lacinia et laoreet viverra class vehicula, sollicitudin quisque primis habitant nisi aenean placerat vehicula platea commodo ligula arcu aliquam ad.Lorem ipsum luctus non nulla accumsan eu rhoncus ut velit sagittis aliquet, morbi posuere interdum bibendum sollicitudin nisi nulla vulputate fringilla augue, etiam felis mauris feugiat massa eu leo torquent sapien aliquam luctus semper dapibus inceptos per quis aliquet urna, facilisis fusce dui placerat potenti taciti, rhoncus iaculis imperdiet venenatis sociosqu id suspendisse condimentum nulla vehicula lacinia et laoreet viverra class vehicula, sollicitudin quisque primis habitant nisi aenean placerat vehicula platea commodo ligula arcu aliquam ad.";
const fakestderr = "Lorem ipsum luctus non nulla accumsan eu rhoncus ut velit sagittis aliquet, morbi posuere interdum bibendum sollicitudin nisi nulla vulputate fringilla augue, etiam felis mauris feugiat massa eu leo torquent sapien aliquam luctus semper dapibus inceptos per quis aliquet urna, facilisis fusce dui placerat potenti taciti, rhoncus iaculis imperdiet venenatis sociosqu id suspendisse condimentum nulla vehicula lacinia et laoreet viverra class vehicula, sollicitudin quisque primis habitant nisi aenean placerat vehicula platea commodo ligula arcu aliquam ad.";

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

    /**
     * Fetch bundle data and update the state of this component.
     * This function will be called by the parent component 'worksheet'.
     */
    refreshBundle = () => {
        // Fetch bundle metadata
        $.ajax({
            type: 'GET',

            url: '/rest/bundles/' + this.props.uuid,
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
            this.setState({ bundleInfo: bundleInfo });
        }).fail(function(xhr, status, err) {
            this.setState({
                bundleInfo: null,
                fileContents: null,
                stdout: null,
                stderr: null,
                errorMessages: this.state.errorMessages.concat([xhr.responseText]),
            });
        });

        // Fetch bundle contents
        $.ajax({
            type: 'GET',
            url: '/rest/bundles/' + this.props.uuid + '/contents/info/',
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
                return this.fetchFileSummary(this.props.uuid, '/').then(function(blob) {
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
                                this.fetchFileSummary(this.props.uuid, '/' + name).then(
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
    };
  
  render(): React.Node {
    const { onClose } = this.props;
    const {
      open,
      bundleInfo,
      errorMessages,
      stdout,
      stderr,
      fileContents } = this.state;

    if (!bundleInfo) {
        return null;
    }

    return (<Drawer
      anchor="bottom"
      open
      onClose={ onClose }
      PaperProps={ { style: {
        minHeight: '75vh',
        width: '90vw',
        maxWidth: 1200,
        borderTopLeftRadius: 8,
        borderTopRightRadius: 8,
        transform: 'translateX(50vw) translateX(-50%)',
      } } }
    >
      <ConfigurationPanel
        buttons={ <BundleActions bundleInfo={ bundleInfo } /> }
        sidebar={ <SideBar bundleInfo={ bundleInfo } /> }
      >
        <MainContent
          bundleInfo={ bundleInfo }
          stdout={ stdout || fakestdout }
          stderr={ stderr || fakestderr }
          fileContents={ fileContents }
        />
      </ConfigurationPanel>
    </Drawer>);
  }
}

export default BundleDetail;
