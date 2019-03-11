import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import _ from 'underscore';
import {
    createDefaultBundleName,
    pathIsArchive,
    getArchiveExt,
    getDefaultBundleMetadata,
    createAlertText,
    createHandleRedirectFn,
} from '../../util/worksheet_utils';
import Button from '../Button';
import ReactDOM from 'react-dom';
import 'jquery-ui-bundle';

const PROGRESS_BAR_ID = 'progressbar-';
const PROGRESS_LABEL_ID = 'progressbar-label-';

type Props = {
    clickAction: 'DEFAULT' | 'SIGN_IN_REDIRECT' | 'DISABLED',
    ws: {},
    reloadWorksheet: () => mixed,
};

class BundleUploader extends React.Component<Props> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({ uploading: {} });
    }

    addUploading(file, bundleUuid) {
        // Append new file to table of uploading bundles
        var key = String(Math.floor(Math.random() * 10000000));
        var entry = {};
        entry[key] = {
            file: file,
            uuid: bundleUuid,
        };
        this.setState(Immutable({ uploading: _.extend(entry, this.state.uploading) }));
        return key;
    }
    clearUploading(key) {
        // Delete entry from table of uploading bundles
        var newUploading = _.clone(this.state.uploading);
        delete newUploading[key];
        this.setState(Immutable({ uploading: newUploading }));
    }
    updateProgress(key, newProgress) {
        var newUploading = _.clone(this.state.uploading);
        newUploading[key].progress = newProgress;
        this.setState(Immutable({ uploading: newUploading }));
    }
    getQueryParams(filename) {
        var formattedFilename = createDefaultBundleName(filename);
        var queryParams = {
            finalize: 1,
            filename: pathIsArchive(filename)
                ? formattedFilename + getArchiveExt(filename)
                : formattedFilename,
            unpack: pathIsArchive(filename) ? 1 : 0,
        };
        return $.param(queryParams);
    }
    uploadBundle = (e) => {
        e.stopPropagation();
        e.preventDefault();
        $(ReactDOM.findDOMNode(this.refs.button)).blur();

        var file = ReactDOM.findDOMNode(this.refs.fileDialog).files[0];
        if (!file) {
            return;
        }
        ReactDOM.findDOMNode(this.refs.fileDialog).value = null;
        var createBundleData = getDefaultBundleMetadata(file.name);
        var self = this;
        $.ajax({
            url: '/rest/bundles?worksheet=' + this.props.ws.info.uuid,
            data: JSON.stringify(createBundleData),
            contentType: 'application/json',
            type: 'POST',
            success: function(data, status, jqXHR) {
                var bundleUuid = data.data[0].id;
                var fileEntryKey = this.addUploading(file.name, bundleUuid);
                var progressbar = $('#' + PROGRESS_BAR_ID + bundleUuid);
                var progressLabel = $('#' + PROGRESS_LABEL_ID + bundleUuid);
                progressbar.progressbar({
                    value: 0,
                    max: 100,
                    create: function() {
                        progressLabel.text(
                            'Uploading ' +
                                createDefaultBundleName(file.name) +
                                '.\n' +
                                '0% completed.',
                        );
                    },
                    change: function() {
                        progressLabel.text(
                            'Uploading ' +
                                createDefaultBundleName(file.name) +
                                '.\n' +
                                progressbar.progressbar('value') +
                                '% completed.',
                        );
                    },
                    complete: function() {
                        progressLabel.text('Waiting for server to finish processing bundle.');
                    },
                });
                var reader = new FileReader();
                reader.onload = function() {
                    var arrayBuffer = this.result,
                        bytesArray = new Uint8Array(arrayBuffer);
                    var url =
                        '/rest/bundles/' +
                        bundleUuid +
                        '/contents/blob/?' +
                        self.getQueryParams(file.name);
                    $.ajax({
                        url: url,
                        type: 'PUT',
                        contentType: 'application/octet-stream',
                        data: new Blob([bytesArray]),
                        processData: false,
                        xhr: function() {
                            var xhr = new window.XMLHttpRequest();
                            xhr.upload.addEventListener(
                                'progress',
                                function(evt) {
                                    if (evt.lengthComputable) {
                                        var percentComplete = parseInt(
                                            (evt.loaded / evt.total) * 100,
                                        );
                                        progressbar.progressbar('value', percentComplete);
                                    }
                                },
                                false,
                            );
                            return xhr;
                        },
                        success: function(data, status, jqXHR) {
                            self.clearUploading(fileEntryKey);
                            self.props.reloadWorksheet();
                        },
                        error: function(jqHXR, status, error) {
                            self.clearUploading(fileEntryKey);
                            alert(
                                createAlertText(
                                    this.url,
                                    jqHXR.responseText,
                                    'refresh and try again.',
                                ),
                            );
                        },
                    });
                };
                reader.readAsArrayBuffer(file);
            }.bind(this),
            error: function(jqHXR, status, error) {
                alert(createAlertText(this.url, jqHXR.responseText));
            }.bind(this),
        });
    };
    openFileDialog = (e) => {
        e.stopPropagation();
        e.preventDefault();

        // Artificially "clicks" on the hidden file input element.
        $(ReactDOM.findDOMNode(this.refs.fileDialog)).trigger('click');
    };
    render() {
        var typeProp, handleClickProp;
        switch (this.props.clickAction) {
            case 'DEFAULT':
                handleClickProp = this.openFileDialog;
                typeProp = 'primary';
                break;
            case 'SIGN_IN_REDIRECT':
                handleClickProp = createHandleRedirectFn(
                    this.props.ws.info ? this.props.ws.info.uuid : null,
                );
                typeProp = 'primary';
                break;
            case 'DISABLED':
                handleClickProp = null;
                typeProp = 'disabled';
                break;
            default:
                break;
        }

        var uploadButton = (
            <Button
                text='Upload'
                type={typeProp}
                handleClick={handleClickProp}
                className='active'
                id='upload-bundle-button'
                innerRef='button'
                flexibleSize={true}
            />
        );

        return (
            <div className='inline-block'>
                {uploadButton}
                <div id='bundle-upload-form' tabIndex='-1' aria-hidden='true'>
                    <form name='uploadForm' encType='multipart/form-data' method='post'>
                        <input
                            id='uploadInput'
                            type='file'
                            ref='fileDialog'
                            name='file'
                            onChange={this.uploadBundle}
                        />
                    </form>
                </div>

                <div id='bundle-upload-progress-bars'>
                    {_.map(this.state.uploading, function(value, key) {
                        var bundleUuid = value.uuid;
                        return (
                            <div
                                id={PROGRESS_BAR_ID + bundleUuid}
                                className='progressbar'
                                key={bundleUuid}
                            >
                                <div
                                    id={PROGRESS_LABEL_ID + bundleUuid}
                                    className='progress-label'
                                />
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    }
}

export default BundleUploader;
