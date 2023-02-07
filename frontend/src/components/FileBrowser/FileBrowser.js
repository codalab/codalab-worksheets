// @flow
import * as React from 'react';
import Paper from '@material-ui/core/Paper';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import FolderIcon from '@material-ui/icons/Folder';
import FileIcon from '@material-ui/icons/InsertDriveFile';
import LinkIcon from '@material-ui/icons/Link';
import { renderSize, shorten_uuid } from '../../util/worksheet_utils';
import './FileBrowser.scss';
import { useState, useEffect, useCallback } from 'react';
import useSWR, { cache } from 'swr';
import { apiWrapper } from '../../util/apiWrapper';
import { formatDirectory } from './utils/formatDirectory';

export class FileBrowser extends React.Component<
    {
        value: string,
        buildPayload: (string) => {},
        method: string,
        url: string,
        onChange?: () => void,
    },
    {
        // Current working directory.
        currentWorkingDirectory: string,

        fileBrowserData: {},
        // FileBrowser has a collapsible-header that can show/hide the content of FileBrowser.
        // isVisible keeps track of whether the content is visible now. If isVisible is false, FileBrowser is collapsed. Vice versa.
        isVisible: false,
    },
> {
    /** Prop default values. */
    static defaultProps = {
        method: 'POST',
    };

    constructor(props) {
        super(props);
        this.state = {
            currentWorkingDirectory: '',
            fileBrowserData: {},
            isVisible: !this.props.startCollapsed,
            hidden: true,
        };
    }

    componentDidMount() {
        if (!this.props.startCollapsed) {
            this.updateFileBrowser('');
        }
    }

    static getDerivedStateFromProps(nextProps, prevState) {
        if (nextProps.isRunBundleUIVisible === false && prevState.isVisible) {
            return { isVisible: false };
        }
    }

    componentDidUpdate(prevProps, prevState) {
        if (this.props.isRunBundleUIVisible === false && prevState.isVisible) {
            this.getDOMNode()
                .getElementsByClassName('file-browser-arrow')[0]
                .click();
        }
        if (prevProps.uuid !== this.props.uuid) {
            // Reset and fire off an asynchronous fetch for new data
            this.setState({ fileBrowserData: {} });
            this.updateFileBrowser('');
        }
    }

    updateFileBrowserWhenDrilledIn = () => {
        if (!this.state.isVisible) {
            this.updateFileBrowser('');
        }
        this.setState({ isVisible: !this.state.isVisible });
    };

    updateFileBrowser = (folder_path) => {
        // folder_path is an absolute path
        if (folder_path === undefined) folder_path = this.state.currentWorkingDirectory;
        this.setState({ currentWorkingDirectory: folder_path });
        const callback = (data) => {
            if (data.data.type === 'directory') {
                this.setState({ fileBrowserData: data.data, hidden: false });
            } else {
                this.setState({ hidden: true });
            }
        };
        const errorHandler = (error) => {
            console.error(error);
            this.setState({ fileBrowserData: {}, hidden: true });
        };
        apiWrapper
            .updateFileBrowser(this.props.uuid, folder_path)
            .then(callback)
            .catch(errorHandler);
    };

    render() {
        let items = [];
        let file_browser;
        if (this.state.fileBrowserData && this.state.fileBrowserData.contents) {
            // Parent directory (..)
            if (this.state.currentWorkingDirectory) {
                items.push(
                    <FileBrowserItem
                        key='..'
                        index='..'
                        type='..'
                        updateFileBrowser={(path) => this.updateFileBrowser(path)}
                        currentWorkingDirectory={this.state.currentWorkingDirectory}
                    />,
                );
            }

            // Sort by name
            let entities = this.state.fileBrowserData.contents;
            entities.sort(function(a, b) {
                if (a.name < b.name) return -1;
                if (a.name > b.name) return +1;
                return 0;
            });
            let self = this;

            // Show directories
            entities.forEach(function(item) {
                if (item.type === 'directory')
                    items.push(
                        <FileBrowserItem
                            bundle_uuid={self.props.uuid}
                            bundle_name={self.props.bundle_name}
                            key={item.name}
                            index={item.name}
                            type={item.type}
                            updateFileBrowser={self.updateFileBrowser}
                            currentWorkingDirectory={self.state.currentWorkingDirectory}
                            hasCheckbox={self.props.hasCheckbox}
                            handleCheckbox={self.props.handleCheckbox}
                        />,
                    );
            });

            // Show files
            entities.forEach(function(item) {
                if (item.type !== 'directory')
                    items.push(
                        <FileBrowserItem
                            bundle_uuid={self.props.uuid}
                            bundle_name={self.props.bundle_name}
                            key={item.name}
                            index={item.name}
                            type={item.type}
                            size={item.size}
                            link={item.link}
                            updateFileBrowser={self.updateFileBrowser}
                            currentWorkingDirectory={self.state.currentWorkingDirectory}
                            hasCheckbox={self.props.hasCheckbox}
                            handleCheckbox={self.props.handleCheckbox}
                        />,
                    );
            });

            file_browser = (
                <table className='file-browser-table'>
                    <tbody>{items}</tbody>
                </table>
            );
        } else {
            file_browser = <b>(no files)</b>;
        }
        let bread_crumbs = (
            <FileBrowserBreadCrumbs
                updateFileBrowser={(path) => this.updateFileBrowser(path)}
                currentWorkingDirectory={this.state.currentWorkingDirectory}
            />
        );
        let content_class_name = this.props.startCollapsed
            ? 'collapsible-content-collapsed'
            : 'collapsible-content';
        let arrow = this.state.isVisible ? (
            <span
                className='file-browser-arrow'
                onClick={() => this.updateFileBrowserWhenDrilledIn()}
            >
                &#x25BE;
            </span>
        ) : (
            <span
                className='file-browser-arrow'
                onClick={() => this.updateFileBrowserWhenDrilledIn()}
            >
                &#x25B8;
            </span>
        );
        let header, checkbox;
        // this.props.hasCheckbox is true in run_bundle_builder for the user to select bundle depedency
        // In other cases, it is false
        if (this.props.hasCheckbox) {
            let url = '/bundles/' + this.props.uuid;
            let short_uuid = shorten_uuid(this.props.uuid);
            checkbox = (
                <input
                    type='checkbox'
                    className='run-bundle-check-box'
                    onChange={this.props.handleCheckbox.bind(
                        this,
                        this.props.uuid,
                        this.props.bundle_name,
                        '',
                    )}
                />
            );
            header = (
                <div className='collapsible-header inline-block'>
                    <a href={url} target='_blank' rel='noopener noreferrer'>
                        {this.props.bundle_name}({short_uuid})
                    </a>
                    &nbsp;{arrow}
                </div>
            );
            bread_crumbs = null;
        } else {
            header = (
                <div className='collapsible-header'>
                    <span>
                        <p>contents {arrow}</p>
                    </span>
                </div>
            );
            checkbox = null;
        }
        return (
            !this.state.hidden && (
                <div className='file-browser'>
                    {checkbox}
                    {header}
                    <div className={content_class_name}>
                        <div className='panel panel-default'>
                            {bread_crumbs}
                            <div className='panel-body'>{file_browser}</div>
                        </div>
                    </div>
                </div>
            )
        );
    }
}

export class FileBrowserBreadCrumbs extends React.Component<{
    updateFileBrowser: (string) => void,
    currentWorkingDirectory: string,
}> {
    render() {
        let links = [];
        let splitDirs = this.props.currentWorkingDirectory.split('/');
        let currentDirectory = '';

        // Generate list of breadcrumbs separated by ' / '
        for (let i = 0; i < splitDirs.length; i++) {
            if (i > 0) currentDirectory += '/';
            currentDirectory += splitDirs[i];
            const fixedCurrentDirectory = currentDirectory;
            links.push(
                <span
                    key={splitDirs[i]}
                    index={splitDirs[i]}
                    onClick={() => this.props.updateFileBrowser(fixedCurrentDirectory)}
                >
                    {' '}
                    / {splitDirs[i]}
                </span>,
            );
        }

        return <div className='panel-heading'>{links}</div>;
    }
}

function encodeBundleContentsPath(path) {
    // Encode each segment of the path separately, because we want to escape
    // everything (such as questions marks) EXCEPT slashes in the path.
    return path
        .split('/')
        .map(encodeURIComponent)
        .join('/');
}

export class FileBrowserItem extends React.Component<{
    bundle_uuid: string,
    updateFileBrowser: (string) => void,
    currentWorkingDirectory: string,
    hasCheckbox?: boolean,
    handleCheckbox?: () => void,
}> {
    render() {
        let size = '';
        let file_location = '';
        if (this.props.type === '..') {
            file_location = this.props.currentWorkingDirectory.substring(
                0,
                this.props.currentWorkingDirectory.lastIndexOf('/'),
            );
        } else if (this.props.currentWorkingDirectory) {
            file_location = this.props.currentWorkingDirectory + '/' + this.props.index;
        } else {
            file_location = this.props.index;
        }
        if (this.props.hasOwnProperty('size')) size = renderSize(this.props.size);
        // this.props.hasCheckbox is true in run_bundle_builder for the user to select bundle depedency
        // otherwise, it is always false
        let checkbox =
            this.props.hasCheckbox && this.props.type !== '..' ? (
                <input
                    className='run-bundle-check-box'
                    type='checkbox'
                    onChange={this.props.handleCheckbox.bind(
                        this,
                        this.props.bundle_uuid,
                        this.props.bundle_name,
                        file_location,
                    )}
                />
            ) : null;

        let item;
        if (this.props.type === 'directory' || this.props.type === '..') {
            item = (
                <span
                    className={this.props.type}
                    onClick={() => this.props.updateFileBrowser(file_location)}
                >
                    <span className='glyphicon-folder-open glyphicon' alt='More' />
                    <button className='link' target='_blank' rel='noopener noreferrer'>
                        {this.props.index}
                    </button>
                    <span className='pull-right'>{size}</span>
                </span>
            );
        } else if (this.props.type === 'file') {
            let file_link =
                '/rest/bundles/' +
                this.props.bundle_uuid +
                '/contents/blob/' +
                encodeBundleContentsPath(file_location);
            item = (
                <span className={this.props.type}>
                    <span className='glyphicon-file glyphicon' alt='More' />
                    <a href={file_link} target='_blank' rel='noopener noreferrer'>
                        {this.props.index}
                    </a>
                    <span className='pull-right'>{size}</span>
                </span>
            );
        } else if (this.props.type === 'link') {
            item = (
                <div className={this.props.type}>
                    <span className='glyphicon-file glyphicon' />
                    {this.props.index + ' → ' + this.props.link}
                </div>
            );
        }

        return (
            <tr>
                <td>
                    {checkbox}
                    {item}
                </td>
            </tr>
        );
    }
}

const rowCenter = {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    cursor: 'pointer',
};

const iconStyle = {
    color: '#aaa',
    marginRight: 4,
};

const fileBrowserItemLiteTableRowStyle = {
    height: 26,
};

class FileBrowserItemLite extends React.Component<{
    bundle_uuid: string,
    updateFileBrowser: (string) => void,
    currentWorkingDirectory: string,
}> {
    render() {
        let size = '';
        let file_location = '';
        if (this.props.type === '..') {
            file_location = this.props.currentWorkingDirectory.substring(
                0,
                this.props.currentWorkingDirectory.lastIndexOf('/'),
            );
        } else if (this.props.currentWorkingDirectory) {
            file_location = this.props.currentWorkingDirectory + '/' + this.props.index;
        } else {
            file_location = this.props.index;
        }
        if (this.props.hasOwnProperty('size')) size = renderSize(this.props.size);

        let item;
        if (this.props.type === 'directory' || this.props.type === '..') {
            item = (
                <TableRow
                    onClick={() => this.props.updateFileBrowser(file_location)}
                    style={fileBrowserItemLiteTableRowStyle}
                >
                    <TableCell>
                        <div style={rowCenter}>
                            <FolderIcon style={iconStyle} />
                            <button className='link' target='_blank' rel='noopener noreferrer'>
                                {this.props.index}
                            </button>
                        </div>
                    </TableCell>
                    <TableCell align='right'>{size}</TableCell>
                </TableRow>
            );
        } else if (this.props.type === 'file') {
            const file_link = `/rest/bundles/${
                this.props.bundle_uuid
            }/contents/blob/${encodeBundleContentsPath(file_location)}`;
            item = (
                <TableRow style={fileBrowserItemLiteTableRowStyle}>
                    <TableCell>
                        <div style={rowCenter}>
                            <FileIcon style={iconStyle} />
                            <a href={file_link} target='_blank' rel='noopener noreferrer'>
                                {this.props.index}
                            </a>
                        </div>
                    </TableCell>
                    <TableCell align='right'>{size}</TableCell>
                </TableRow>
            );
        } else if (this.props.type === 'link') {
            item = (
                <TableRow style={fileBrowserItemLiteTableRowStyle}>
                    <TableCell>
                        <div style={rowCenter}>
                            <LinkIcon style={iconStyle} />
                            {this.props.index + ' → ' + this.props.link}
                        </div>
                    </TableCell>
                    <TableCell />
                </TableRow>
            );
        }

        return item;
    }
}

const breadcrumbStyle = {
    padding: '5px 24px',
    fontFamily: 'Roboto',
    fontWeight: 500,
    backgroundColor: '#F1F3F5', // grey.lighter
    color: '#8895A7', // grey.darker
    borderBottom: '1px solid rgba(224, 224, 224, 1)', // match table borders
};

export const FileBrowserLite = ({
    uuid,
    startCollapsed,
    isRunningBundle,
    bundle_name,
    showBreadcrumbs,
}) => {
    const [currentWorkingDirectory, setCurrentWorkingDirectory] = useState('');
    const [fileBrowserData, setFileBrowserData] = useState({});
    const [hidden, setHidden] = useState(true);

    const displayFileBrowser = (data) => {
        if (data.data.type === 'directory') {
            setFileBrowserData(data.data);
            setHidden(false);
        } else {
            setHidden(true);
        }
    };

    const updateFileBrowser = useCallback(
        (folder_path) => {
            // folder_path is an absolute path
            if (folder_path === undefined) folder_path = currentWorkingDirectory;
            setCurrentWorkingDirectory(folder_path);
        },
        [currentWorkingDirectory],
    );

    useEffect(() => {
        setFileBrowserData({});
        setCurrentWorkingDirectory('');
    }, [uuid]);

    useEffect(() => {
        if (cache.has(url)) {
            const data = cache.get(url);
            displayFileBrowser(data);
        }
    });

    const fetcher = (url) =>
        fetch(url, {
            type: 'GET',
            url: url,
            dataType: 'json',
        })
            .then((r) => {
                return r.json();
            })
            .catch(() => {
                setFileBrowserData({});
                setHidden(true);
            });

    const url =
        '/rest/bundles/' +
        uuid +
        '/contents/info/' +
        currentWorkingDirectory +
        '?' +
        new URLSearchParams({ depth: 1 });
    useSWR(url, fetcher, {
        revalidateOnMount: true,
        refreshInterval: isRunningBundle ? 4000 : 0,
        onSuccess: (data, key, config) => {
            displayFileBrowser(data);
        },
    });

    const entities = (fileBrowserData || {}).contents;
    if (!entities) {
        return null;
    }
    entities.sort(function(a, b) {
        if (a.name < b.name) return -1;
        if (a.name > b.name) return +1;
        return 0;
    });
    const items = [];
    // Show parent directory.
    if (currentWorkingDirectory) {
        items.push(
            <FileBrowserItemLite
                key='..'
                index='..'
                type='..'
                updateFileBrowser={(path) => updateFileBrowser(path)}
                currentWorkingDirectory={currentWorkingDirectory}
            />,
        );
    }

    // Show directories
    entities.forEach((item) => {
        if (item.type === 'directory')
            items.push(
                <FileBrowserItemLite
                    bundle_uuid={uuid}
                    bundle_name={bundle_name}
                    key={item.name}
                    index={item.name}
                    type={item.type}
                    updateFileBrowser={updateFileBrowser}
                    currentWorkingDirectory={currentWorkingDirectory}
                />,
            );
    });

    // Show files
    entities.forEach((item) => {
        if (item.type !== 'directory')
            items.push(
                <FileBrowserItemLite
                    bundle_uuid={uuid}
                    bundle_name={bundle_name}
                    key={item.name}
                    index={item.name}
                    type={item.type}
                    size={item.size}
                    link={item.link}
                    updateFileBrowser={updateFileBrowser}
                    currentWorkingDirectory={currentWorkingDirectory}
                />,
            );
    });
    return (
        hidden || (
            <Paper>
                {showBreadcrumbs && currentWorkingDirectory && (
                    <div style={breadcrumbStyle}>{formatDirectory(currentWorkingDirectory)}</div>
                )}
                <Table style={{ backgroundColor: 'white' }}>
                    <TableBody>{items}</TableBody>
                </Table>
            </Paper>
        )
    );
};
