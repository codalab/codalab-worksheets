import * as React from 'react';
import { withStyles } from '@material-ui/core/styles';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import BundleDetail from '../BundleDetail';


class ImageItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
        };
    }

    handleClick = () => {
        this.props.setFocus(this.props.focusIndex, 0);
    };

    render() {
        if (this.props.focused) {
            // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['enter'],
                (e) => {
                    e.preventDefault();
                    if (!this.props.confirmBundleRowAction(e.code)) {
                        this.setState({showDetail: !this.state.showDetail });
                    }
                },
                'keydown',
            );
            // unbind shortcuts that are active for markdown_block and worksheet_block
            Mousetrap.unbind('i');
        }
        const {classes} = this.props;
        var item = this.props.item;
        var bundleInfo = item.bundles_spec.bundle_infos[0];
        var className = 'type-image' + (this.props.focused ? ' focused' : '');
        var src = 'data:image/png;base64,' + this.props.item.image_data;
        var styles = {};
        if (this.props.item.hasOwnProperty('height')) {
            styles['height'] = this.props.item.height + 'px';
        }
        if (this.props.item.hasOwnProperty('width')) {
            styles['width'] = this.props.item.width + 'px';
        }
        return (
            <div className='ws-item' onClick={this.handleClick}>
                <div className={className} ref={this.props.item.ref}>
                    <img style={styles} src={src} />
                </div>
                {this.state.showDetail &&
                    <BundleDetail
                        uuid={bundleInfo.uuid}
                        ref='bundleDetail'
                        onClose={() => {
                            this.setState({
                                showDetail: false,
                            });
                        }}
                        isFocused={this.props.focused}
                        focusIndex={this.props.focusIndex}
                        showDetail={this.state.showDetail}
                        editPermission={this.props.editPermission}
                    />
                }
            </div>
        );
    }
}

const styles = (theme) => ({
    tableBody: {
        '&:hover $rightButtonStripe': {
            display: 'flex',
        },
    },
    rightButtonStripe: {
        display: 'none',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        left: '100%',
        transform: 'translateY(-100%) translateX(-100%)',
    },
    rootNoPad: {
        verticalAlign: 'middle !important',
        border: 'none !important',
        padding: '0px !important',
        wordWrap: 'break-word',
        maxWidth: 200,
        minWidth: 100,
    },
    bundleDetail: {
        paddingLeft: `${theme.spacing.largest}px !important`,
        paddingRight: `${theme.spacing.largest}px !important`,
    },
    contentRow: {
        height: 26,
        borderBottom: '2px solid #ddd',
        borderLeft: '3px solid transparent',
        padding: 0,
        '&:hover': {
            boxShadow:
                'inset 1px 0 0 #dadce0, inset -1px 0 0 #dadce0, 0 1px 2px 0 rgba(60,64,67,.3), 0 1px 3px 1px rgba(60,64,67,.15)',
            zIndex: 1,
        },
    },
    checkBox: {
        '&:hover': {
            backgroundColor: '#ddd',
        },
    },
    highlight: {
        backgroundColor: `${theme.color.primary.lightest} !important`,
        borderLeft: '3px solid #1d91c0',
    },
    lowlight: {
        backgroundColor: `${theme.color.grey.light} !important`,
    },
});

export default withStyles(styles)(ImageItem);
