import React from 'react';
import 'rc-slider/assets/index.css';
import { withStyles } from '@material-ui/core/styles';
import { getQueryParams } from '../NewUpload/NewUpload.jsx';
import $ from 'jquery';
import { createAlertText, getDefaultBundleMetadata } from '../../../util/worksheet_utils';
import { FILE_SIZE_LIMIT_B, FILE_SIZE_LIMIT_GB } from '../../../constants';

const styles = (theme) => ({});

class ImageEditor extends React.Component<{
    /** JSS styling object. */
    classes: {},

    /** The worksheet to insert into **/
    worksheetUUID: string,

    /** Insert after this bundle **/
    after_sort_key: string,
}> {
    state = {
        /* Whether the upload is in progress */
        uploading: false,
    };

    inputFile = React.createRef();

    setFile = () => {
        const files = this.inputFile.current.files;

        if (!files.length) {
            return;
        }
        this.uploadFiles(files);
    };

    readFileAsync(bundleUuid, file) {
        return new Promise((resolve, reject) => {
            let reader = new FileReader();
            reader.onload = () => {
                let arrayBuffer = reader.result,
                    bytesArray = new Uint8Array(arrayBuffer);
                let url =
                    '/rest/bundles/' + bundleUuid + '/contents/blob/?' + getQueryParams(file.name);
                $.ajax({
                    url: url,
                    type: 'PUT',
                    contentType: 'application/octet-stream',
                    data: new Blob([bytesArray]),
                    processData: false,
                    success: function(data) {
                        resolve(data);
                    },
                    error: function(error) {
                        this.clearProgress();
                        alert(createAlertText(error.responseText, 'refresh and try again.'));
                        this.props.onUploadFinish();
                        reject(error);
                    }.bind(this),
                });
            };
            reader.readAsArrayBuffer(file);
        });
    }

    asyncUploadFiles = async (files) => {
        const { worksheetUUID, after_sort_key } = this.props;
        const { name, description } = this.state;
        this.setState({
            uploading: true,
        });

        let promises = [...files].map(async (file) => {
            const createBundleData = getDefaultBundleMetadata(name || file.name, description);
            let url = `/rest/bundles?worksheet=${worksheetUUID}`;
            if (after_sort_key || after_sort_key === 0) {
                url += `&after_sort_key=${after_sort_key}`;
            }

            async function createFileBundle(url, data) {
                let result;
                try {
                    result = await $.ajax({
                        url: url,
                        data: data,
                        contentType: 'application/json',
                        type: 'POST',
                    });
                    return result;
                } catch (error) {
                    this.clearProgress();
                    alert(createAlertText(url, error.responseText));
                }
            }

            const bundle = await createFileBundle(url, JSON.stringify(createBundleData));
            const bundleUuid = bundle.data[0].id;
            const promise = await this.readFileAsync(bundleUuid, file);
            return promise;
        });
        const moveIndex = true;
        const addImage = true;
        const param = { moveIndex, addImage };
        await Promise.all(promises);
        this.clearProgress();
        this.props.reloadWorksheet(undefined, undefined, param);
        this.props.onUploadFinish();
    };

    uploadFiles = (files) => {
        if (!files) {
            return;
        }

        let fileSize = 0;
        for (const file of files) {
            fileSize += file.size;
        }

        if (fileSize > FILE_SIZE_LIMIT_B) {
            alert(
                'Image size is large than ' +
                    FILE_SIZE_LIMIT_GB +
                    'GB. Please upload your image(s) through CLI.',
            );
            return;
        }
        this.asyncUploadFiles(files);
    };

    clearProgress = () => {
        this.setState({ uploading: false });
    };

    render() {
        return (
            <React.Fragment>
                <input
                    type='file'
                    id='codalab-image-upload-input'
                    multiple={false} // only support uploading one image per time
                    style={{ display: 'none' }}
                    ref={this.inputFile}
                    onChange={this.setFile}
                    accept='image/*'
                    key={this.props.after_sort_key} // force to update the imageEditor component when after_sort_key changes
                />
            </React.Fragment>
        );
    }
}

export default withStyles(styles)(ImageEditor);
