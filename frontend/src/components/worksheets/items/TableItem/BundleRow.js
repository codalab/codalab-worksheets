import * as React from 'react';
import { withStyles } from '@material-ui/core';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import TableBody from '@material-ui/core/TableBody';
import BundleDetail from '../../BundleDetail';

class BundleRow extends React.Component<
    {
        onEnter: () => void,
        onLeave: () => void,
    }

>{
    static defaultProps = {
        onEnter: () => {},
        onLeave: () => {},
    }

    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
        };
    }

    handleClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
        const { showDetail } = this.state;
        this.setState({
            showDetail: !showDetail,
        });
    };

    render() {
        const { bundleInfo, classes, onMouseMove } = this.props;
        const { showDetail } = this.state;
        var rowItems = this.props.item;
        var baseUrl = this.props.url;
        var uuid = this.props.uuid;
        var columnWithHyperlinks = this.props.columnWithHyperlinks;
        var rowCells = this.props.headerItems.map(function(headerKey, col) {
            var rowContent = rowItems[headerKey];

            // See if there's a link
            var url;
            if (col == 0) {
                url = baseUrl;
            } else if (columnWithHyperlinks.indexOf(headerKey) != -1) {
                url = '/rest/bundles/' + uuid + '/contents/blob' + rowContent['path'];
                if ('text' in rowContent) {
                    rowContent = rowContent['text'];
                } else {
                    // In case text doesn't exist, content will default to basename of the path
                    // indexing 1 here since the path always starts with '/'
                    rowContent = rowContent['path'].split('/')[1];
                }
            }
            if (url)
                rowContent = (
                    <a href={url} className='bundle-link' target='_blank'>
                        {rowContent}
                    </a>
                );
            else rowContent = rowContent + '';

            return (
                <TableCell
                    key={col}
                    classes={ {
                        root: showDetail ? classes.noBottomBorder : classes.root
                    } }
                >
                    {rowContent}
                </TableCell>
            );
        });

        return (
            <TableBody onMouseMove={ onMouseMove }>
                <TableRow
                    onClick={this.handleClick}
                    onContextMenu={this.props.handleContextMenu.bind(
                        null,
                        bundleInfo.uuid,
                        this.props.focusIndex,
                        this.props.rowIndex,
                        bundleInfo.bundle_type === 'run',
                    )}
                >
                    {rowCells}
                </TableRow>
                {
                    showDetail &&
                    <TableRow>
                        <TableCell colspan="100%" classes={ { root: classes.noTopBorder  } } >
                            <BundleDetail
                                uuid={ bundleInfo.uuid }
                                bundleMetadataChanged={ this.props.reloadWorksheet }
                                ref='bundleDetail'
                                onClose={ () => {
                                    this.setState({
                                        showDetail: false,
                                    });
                                } }
                            />
                        </TableCell>
                    </TableRow>
                }
            </TableBody>
        );
    }
}

const styles = (theme) => ({
    root: {
        verticalAlign: 'middle !important',
    },
    noTopBorder: {
        borderTop: 'none !important',
        padding: 0,
    },
    noBottomBorder: {
        verticalAlign: 'middle !important',
        borderBottom: 'none',
    },
});

export default withStyles(styles)(BundleRow);
