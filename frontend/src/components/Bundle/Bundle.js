// @flow
import * as React from 'react';
import SubHeader from '../SubHeader';
import ContentWrapper from '../ContentWrapper';
import { JsonApiDataStore } from 'jsonapi-datastore';
import { renderFormat, renderPermissions, shorten_uuid } from '../../util/worksheet_utils';
import { BundleEditableField } from '../EditableField';
import { FileBrowser } from '../FileBrowser/FileBrowser';
import './Bundle.scss';
import ErrorMessage from '../worksheets/ErrorMessage';
import { fetchBundleContents, fetchBundleMetadata, fetchFileSummary } from '../../util/apiWrapper';

class Bundle extends React.Component<
    {
        // UUID of bundle.
        uuid: string,

        // Callback on metadata change.
        bundleMetadataChanged: () => void,

        // Whether this bundle is displayed in full page.
        isStandalonePage: boolean,
    },
    {
        errorMessages: string[],
        bundleInfo: {},
        fileContents: string,
        stdout: string,
        stderr: string,
    },
> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            errorMessages: [],
            bundleInfo: null,
            fileContents: null,
            stdout: null,
            stderr: null,
            prevUuid: props.uuid,
        };
    }

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

    /**
     * Fetch bundle data and update the state of this component.
     */
    refreshBundle = () => {
        // Fetch bundle metadata
        let callback = (response) => {
            // Normalize JSON API doc into simpler object
            const bundleInfo = new JsonApiDataStore().sync(response);
            bundleInfo.editableMetadataFields = response.data.meta.editable_metadata_keys;
            bundleInfo.metadataType = response.data.meta.metadata_type;
            this.setState({ bundleInfo: bundleInfo });
        };

        let errorHandler = (error) => {
            if (error.response && error.response.status === 404) {
                this.setState({
                    fileContents: null,
                    stdout: null,
                    stderr: null,
                });
            } else {
                this.setState({
                    bundleInfo: null,
                    fileContents: null,
                    stdout: null,
                    stderr: null,
                    errorMessages: this.state.errorMessages.concat([error]),
                });
            }
        };

        fetchBundleMetadata(this.props.uuid)
            .then(callback)
            .catch(errorHandler);

        // Fetch bundle contents
        callback = async (response) => {
            const info = response.data;
            if (!info) return;
            if (info.type === 'file' || info.type === 'link') {
                return fetchFileSummary(this.props.uuid, '/').then((blob) => {
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
                                fetchFileSummary(this.props.uuid, '/' + name).then((blob) => {
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

        fetchBundleContents(this.props.uuid)
            .then(callback)
            .catch(errorHandler);
    };

    componentDidMount() {
        if (this.props.isStandalonePage) {
            this.refreshBundle();
        }
    }

    /** Renderer. */
    render() {
        const bundleInfo = this.state.bundleInfo;
        if (!bundleInfo) {
            // Error
            if (this.state.errorMessages.length > 0) {
                return <ErrorMessage message={"Not found: '/bundles/" + this.props.uuid + "'"} />;
            }

            // Still loading
            return (
                <div id='bundle-message' className='bundle-detail'>
                    <img alt='Loading' src={`${process.env.PUBLIC_URL}/img/Preloader_Small.gif`} />{' '}
                    Loading bundle...
                </div>
            );
        }

        const bundleMetadataChanged = this.props.isStandalonePage
            ? this.refreshBundle
            : this.props.bundleMetadataChanged;

        const content = (
            <div id='panel_content'>
                {renderErrorMessages(this.state.errorMessages)}
                {renderHeader(bundleInfo, bundleMetadataChanged)}
                {renderDependencies(bundleInfo)}
                {renderContents(
                    bundleInfo,
                    this.state.fileContents,
                    this.state.stdout,
                    this.state.stderr,
                )}
                <FileBrowser uuid={bundleInfo.uuid} />
                {renderMetadata(bundleInfo, bundleMetadataChanged)}
                {renderHostWorksheets(bundleInfo)}
            </div>
        );

        if (this.props.isStandalonePage) {
            return (
                <div id='bundle-content'>
                    <React.Fragment>
                        <SubHeader title='Bundle View' />
                        <ContentWrapper>{content}</ContentWrapper>
                    </React.Fragment>
                </div>
            );
        } else {
            return content;
        }
    }
}

// TODO: all of these should be a part of the bundle, or their own pure react components, not random functions

function renderErrorMessages(messages) {
    return (
        <div id='bundle-error-messages'>
            {messages.map((message) => {
                return <div className='alert alert-danger alert-dismissable'>{message}</div>;
            })}
        </div>
    );
}

function renderDependencies(bundleInfo) {
    let dependencies_table = [];
    if (!bundleInfo.dependencies || bundleInfo.dependencies.length === 0) return <div />;

    bundleInfo.dependencies.forEach(function(dep, i) {
        let dep_bundle_url = '/bundles/' + dep.parent_uuid;
        dependencies_table.push(
            <tr key={dep.parent_uuid + i}>
                <td>{dep.child_path}</td>
                <td>
                    &rarr; {dep.parent_name}(
                    <a href={dep_bundle_url}>{shorten_uuid(dep.parent_uuid)}</a>)
                    {dep.parent_path ? '/' + dep.parent_path : ''}
                </td>
            </tr>,
        );
    });

    return (
        <div>
            <h4>dependencies</h4>
            <table className='bundle-meta table'>
                <tbody>{dependencies_table}</tbody>
            </table>
        </div>
    );
}

function createRow(bundleInfo, bundleMetadataChanged, key, value) {
    // Return a row corresponding to showing
    //   key: value
    // which can be edited.
    let editableMetadataFields = bundleInfo.editableMetadataFields;
    let fieldType = bundleInfo.metadataType;
    if (
        bundleInfo.permission > 1 &&
        editableMetadataFields &&
        editableMetadataFields.indexOf(key) !== -1
    ) {
        return (
            <tr key={key}>
                <th>
                    <span className='editable-key'>{key}</span>
                </th>
                <td>
                    <BundleEditableField
                        canEdit={true}
                        dataType={fieldType[key]}
                        fieldName={key}
                        uuid={bundleInfo.uuid}
                        value={value}
                        onChange={bundleMetadataChanged}
                    />
                </td>
            </tr>
        );
    } else {
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
}

function renderMetadata(bundleInfo, bundleMetadataChanged) {
    let metadata = bundleInfo.metadata;
    let metadataListHtml = [];

    // FIXME: editing allow_failed_dependencies doesn't work
    // FIXME: merge with other switch statements?
    // FIXME: use simpler declarative setup instead of looping and switches?
    // Sort the metadata by key.
    let keys = [];
    for (let property in metadata) {
        if (metadata.hasOwnProperty(property)) keys.push(property);
    }
    keys.sort();
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        metadataListHtml.push(createRow(bundleInfo, bundleMetadataChanged, key, metadata[key]));
    }

    return (
        <div>
            <div className='collapsible-header'>
                <span>
                    <p>metadata &#x25BE;</p>
                </span>
            </div>
            <div className='collapsible-content'>
                <table className='bundle-meta table'>
                    <tbody>{metadataListHtml}</tbody>
                </table>
            </div>
        </div>
    );
}

function renderHeader(bundleInfo, bundleMetadataChanged) {
    let bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
    let bundleStateClass = 'bundle-state state-' + (bundleInfo.state || 'ready');

    // Display basic information
    let rows = [];
    rows.push(createRow(bundleInfo, bundleMetadataChanged, 'uuid', bundleInfo.uuid));
    rows.push(createRow(bundleInfo, bundleMetadataChanged, 'name', bundleInfo.metadata.name));
    rows.push(
        createRow(
            bundleInfo,
            bundleMetadataChanged,
            'description',
            bundleInfo.metadata.description,
        ),
    );
    rows.push(
        createRow(
            bundleInfo,
            bundleMetadataChanged,
            'owner',
            bundleInfo.owner === null ? '<anonymous>' : bundleInfo.owner.user_name,
        ),
    );
    rows.push(
        createRow(
            bundleInfo,
            bundleMetadataChanged,
            'is_anonymous',
            renderFormat(bundleInfo.is_anonymous, 'bool'),
        ),
    );
    rows.push(
        createRow(bundleInfo, bundleMetadataChanged, 'permissions', renderPermissions(bundleInfo)),
    );
    rows.push(createRow(bundleInfo, bundleMetadataChanged, 'created', bundleInfo.metadata.created));
    rows.push(
        createRow(bundleInfo, bundleMetadataChanged, 'data_size', bundleInfo.metadata.data_size),
    );
    if (bundleInfo.bundle_type === 'run') {
        rows.push(createRow(bundleInfo, bundleMetadataChanged, 'command', bundleInfo.command));
    }
    if (bundleInfo.metadata.failure_message) {
        rows.push(
            createRow(
                bundleInfo,
                bundleMetadataChanged,
                'failure_message',
                bundleInfo.metadata.failure_message,
            ),
        );
    }

    rows.push(
        createRow(
            bundleInfo,
            bundleMetadataChanged,
            'state',
            <span className={bundleStateClass}>{bundleInfo.state}</span>,
        ),
    );

    if (bundleInfo.bundle_type === 'run') {
        if (bundleInfo.metadata.run_status)
            rows.push(
                createRow(
                    bundleInfo,
                    bundleMetadataChanged,
                    'run_status',
                    bundleInfo.metadata.run_status,
                ),
            );
        rows.push(createRow(bundleInfo, bundleMetadataChanged, 'time', bundleInfo.metadata.time));
    }

    let bundleHeader;
    // TODO: don't use the fact that there is a bundle-content element in lgoic
    if (document.getElementById('bundle-content')) {
        let bundle_name = <h3 className='bundle-name'>{bundleInfo.metadata.name}</h3>;
        bundleHeader = (
            <div className='bundle-header'>
                {bundle_name}
                <div className='bundle-links'>
                    <a
                        href={bundleDownloadUrl}
                        className='bundle-download btn btn-default btn-sm'
                        alt='Download Bundle'
                    >
                        <span className='glyphicon glyphicon-download-alt' />
                    </a>
                </div>
            </div>
        );
    }
    return (
        <div>
            {bundleHeader}
            <table className='bundle-meta table'>
                <tbody>
                    {rows.map(function(elem) {
                        return elem;
                    })}
                    <tr>
                        <th>
                            <span>download</span>
                        </th>
                        <td>
                            <div className='bundle-links'>
                                <a
                                    href={bundleDownloadUrl}
                                    className='bundle-download btn btn-default btn-sm'
                                    alt='Download Bundle'
                                >
                                    <span className='glyphicon glyphicon-download-alt' />
                                </a>
                            </div>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    );
}

function renderContents(bundleInfo, fileContents, stdout, stderr) {
    let stdoutHtml = '';
    if (stdout) {
        let stdoutUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/stdout';
        stdoutHtml = (
            <div>
                <span>
                    <a href={stdoutUrl} target='_blank' rel='noopener noreferrer'>
                        stdout
                    </a>
                </span>
                &nbsp;
                <span className='collapsible-header'>&#x25BE;</span>
                <div className='collapsible-content bundle-meta'>
                    <pre>{stdout}</pre>
                </div>
            </div>
        );
    }

    let stderrHtml = '';
    if (stderr) {
        let stderrUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/stderr';
        stderrHtml = (
            <div>
                <span>
                    <a href={stderrUrl} target='_blank' rel='noopener noreferrer'>
                        stderr
                    </a>
                </span>
                &nbsp;
                <span className='collapsible-header'>&#x25BE;</span>
                <div className='collapsible-content bundle-meta'>
                    <pre>{stderr}</pre>
                </div>
            </div>
        );
    }

    let contentsHtml = '';
    if (fileContents) {
        contentsHtml = (
            <div>
                <div className='collapsible-header'>
                    <span>
                        <p>contents &#x25BE;</p>
                    </span>
                </div>
                <div className='collapsible-content bundle-meta'>
                    <pre>{fileContents}</pre>
                </div>
            </div>
        );
    }

    return (
        <div>
            {contentsHtml}
            {stdoutHtml}
            {stderrHtml}
        </div>
    );
}

function renderHostWorksheets(bundleInfo) {
    if (!bundleInfo.host_worksheets) return <div />;

    let hostWorksheetRows = [];
    bundleInfo.host_worksheets.forEach(function(worksheet) {
        let hostWorksheetUrl = '/worksheets/' + worksheet.uuid;
        hostWorksheetRows.push(
            <tr key={worksheet.uuid}>
                <td>
                    <a href={hostWorksheetUrl}>{worksheet.name}</a>
                </td>
            </tr>,
        );
    });

    return (
        <div>
            <div className='collapsible-header'>
                <span>
                    <p>host worksheets &#x25BE;</p>
                </span>
            </div>
            <div className='collapsible-content'>
                <div className='host-worksheets-table'>
                    <table className='bundle-meta table'>
                        <tbody>{hostWorksheetRows}</tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}

export default Bundle;
