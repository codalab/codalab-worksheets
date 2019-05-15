import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import _ from 'underscore';
import { renderPermissions, shorten_uuid } from '../../util/worksheet_utils';
import { WorksheetEditableField } from '../EditableField';
import Bundle from '../Bundle';
import BundleDetail from './BundleDetail';
import NewUpload from './NewUpload';
import NewRun from './NewRun';
import RunBundleBuilder from './RunBundleBuilder';
import NewWorksheet from './NewWorksheet';
import BundleUploader from './BundleUploader';

class WorksheetSidePanel extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    componentDidMount(e) {
        var self = this;
        $('#dragbar_vertical').mousedown(function(e) {
            self.resizePanel(e);
        });
        $(document).mouseup(function(e) {
            $(this).unbind('mousemove');
        });
        $(window).resize(function(e) {
            self.resetPanel();
        });
    }

    componentWillMount() {
        this.debouncedRefreshBundleSidePanel = _.debounce(this.refreshBundleSidePanel, 200).bind(
            this,
        );
        this.debouncedRefreshWorksheetSidePanel = _.debounce(
            this.reloadWorksheetSidePanel,
            200,
        ).bind(this);
    }

    refreshBundleSidePanel() {
        if (this.refs.hasOwnProperty('bundle_info_side_panel')) {
            this.refs.bundle_info_side_panel.refreshBundle();
        }
    }

    reloadWorksheetSidePanel() {
        if (this.refs.hasOwnProperty('worksheet_info_side_panel')) {
            this.refs.worksheet_info_side_panel.reloadWorksheet();
        }
    }

    componentDidUpdate() {
        this.debouncedRefreshBundleSidePanel();
        this.debouncedRefreshWorksheetSidePanel();
    }

    getFocus() {
        // Return the state to show on the side panel
        var info = this.props.ws.info;
        if (!info) return null;
        if (this.props.focusIndex == -1)
            // Not focus on anything, show worksheet
            return info;
        return info.items[this.props.focusIndex];
    }

    // What kind of thing is it?
    isFocusWorksheet(focus) {
        return focus.mode === undefined || focus.mode == 'subworksheets_block';
    }
    isFocusMarkup(focus) {
        // If search and didn't return bundles, then count as markup
        return focus.mode == 'markup_block';
    }
    isFocusBundle(focus) {
        return !this.isFocusWorksheet(focus) && !this.isFocusMarkup(focus);
    }
    getBundleInfo(focus) {
        if (focus.mode == 'table_block')
            // Drill down into row of table
            return this.props.subFocusIndex != -1
                ? focus.bundles_spec.bundle_infos[this.props.subFocusIndex]
                : null;
        else return focus.bundles_spec.bundle_infos[0];
    }
    getWorksheetInfo(focus) {
        if (focus.mode === 'subworksheets_block') {
            if (this.props.subFocusIndex === -1) return null;
            var item = focus.subworksheet_infos[this.props.subFocusIndex];
            return item;
        } else return focus;
    }

    resizePanel(e) {
        e.preventDefault();
        $(document).mousemove(function(e) {
            var windowWidth = $(window).width();
            var panelWidth = windowWidth - e.pageX;
            var panelWidthPercentage = ((windowWidth - e.pageX) / windowWidth) * 100;
            if (240 < panelWidth && panelWidthPercentage < 55) {
                $('.ws-container').css('width', e.pageX);
                $('.ws-panel').css('width', panelWidthPercentage + '%');
                $('#dragbar_vertical').css('right', panelWidthPercentage + '%');
            }
        });
    }
    resetPanel() {
        var windowWidth = $(window).width();
        if (windowWidth < 768) {
            $('.ws-container').removeAttr('style');
        } else {
            var panelWidth = parseInt($('.ws-panel').css('width'));
            var containerWidth = windowWidth - panelWidth;
            $('.ws-container').css('width', containerWidth);
        }
    }

    render() {
        /**
         * - If the user isn't logged in, show the buttons as they would
         *   normally, but redirect the user to the sign in page if they
         *   click the buttons
         * - If the user is logged in but they don't have permissions
         *   to write to this worksheet, show a grayed out version
         *   of the button
         * - If the user is logged in and has permissions, let
         *   them use the upload / new run / new worksheet functionality
         */
        var clickAction = 'DEFAULT';
        if (!this.props.userInfo) {
            clickAction = 'SIGN_IN_REDIRECT';
        } else if (this.props.ws.info && !this.props.ws.info.edit_permission) {
            clickAction = 'DISABLED';
        }

        var focus = this.getFocus();
        var side_panel_details = null;
        if (focus) {
            if (this.isFocusWorksheet(focus)) {
                // Show worksheet (either main worksheet or subworksheet)
                var worksheet_info = this.getWorksheetInfo(focus);

                side_panel_details = (
                    <WorksheetDetailSidePanel
                        key={'ws' + this.props.focusIndex}
                        worksheet_info={worksheet_info}
                        bundleMetadataChanged={this.props.bundleMetadataChanged}
                        ref='worksheet_info_side_panel'
                    />
                );
            } else if (this.isFocusMarkup(focus)) {
                // Show nothing (maybe later show markdown just for fun?)
            } else if (this.isFocusBundle(focus)) {
                // Show bundle (either full bundle or row in table)
                // var bundle_info = this.getBundleInfo(focus);
                // if (bundle_info) {
                //     side_panel_details = (
                //         <BundleDetail
                //             uuid={bundle_info.uuid}
                //             bundleMetadataChanged={this.props.bundleMetadataChanged}
                //             ref='bundle_info_side_panel'
                //             onClose={ this.props.deFocus }
                //         />
                //     );
                // }
            } else {
                console.error('Unknown mode: ' + focus.mode);
            }
        }

        return (
            <div className='ws-panel'>
                <div className='ws-button-group'>
                    <NewUpload />
                    <NewRun />
                    <BundleUploader
                        clickAction={clickAction}
                        ws={this.props.ws}
                        reloadWorksheet={this.props.bundleMetadataChanged}
                    />
                    <RunBundleBuilder
                        clickAction={clickAction}
                        ws={this.props.ws}
                        escCount={this.props.escCount}
                    />
                    <NewWorksheet
                        clickAction={clickAction == 'DISABLED' ? 'DEFAULT' : clickAction}
                        escCount={this.props.escCount}
                        userInfo={this.props.userInfo}
                        ws={this.props.ws}
                    />
                </div>
                {side_panel_details}
            </div>
        );
    }
}

// When selecting a worksheet.
class WorksheetDetailSidePanel extends React.Component {
    reloadWorksheet() {
        var ws = this.props.worksheet_info;
        var onSuccess = function(result, status, xhr) {
            this.setState(result);
        }.bind(this);
        var onError = function(xhr, status, error) {
            console.error(xhr.responseText);
        }.bind(this);
        $.ajax({
            type: 'GET',
            url: '/rest/interpret/worksheet/' + ws.uuid,
            // TODO: migrate to using main API
            // url: '/rest/worksheets/' + ws.uuid,
            success: onSuccess,
            error: onError,
        });
    }

    render() {
        // Select the current worksheet or the subworksheet.
        var worksheet = this.state;
        var isEmptyObject = function(obj) {
            // based on: http://stackoverflow.com/questions/4994201/is-object-empty
            for (var key in obj) {
                if (hasOwnProperty.call(obj, key)) return false;
            }
            return true;
        };
        if (isEmptyObject(worksheet)) worksheet = this.props.worksheet_info;

        if (!worksheet) return <div />;

        // Show brief summary of contents.
        var rows = [];
        if (worksheet.items) {
            worksheet.items.forEach(function(item, itemIndex) {
                if (item.bundles_spec) {
                    // Show bundle
                    var bundle_infos = item.bundles_spec.bundle_infos;

                    bundle_infos.forEach(function(b, bundleItemIndex) {
                        var url = '/bundles/' + b.uuid;
                        var short_uuid = shorten_uuid(b.uuid);
                        rows.push(
                            <tr key={short_uuid + itemIndex + bundleItemIndex}>
                                <td>{b.bundle_type}</td>
                                <td>
                                    <a href={url} target='_blank'>
                                        {b.metadata.name}({short_uuid})
                                    </a>
                                </td>
                            </tr>,
                        );
                    });
                } else if (item.mode == 'subworksheets_block') {
                    // Show worksheet
                    item.subworksheet_infos.forEach(function(info, subworksheetItemIndex) {
                        var title = info.title || info.name;
                        var url = '/worksheets/' + info.uuid;
                        rows.push(
                            <tr key={url + itemIndex + subworksheetItemIndex}>
                                <td>worksheet</td>
                                <td>
                                    <a href={url} target='_blank'>
                                        {title}
                                    </a>
                                </td>
                            </tr>,
                        );
                    });
                }
            });
        }

        var bundles_html = (
            <div className='bundles-table'>
                <table className='bundle-meta table'>
                    <thead>
                        <tr>
                            <th>type</th>
                            <th>name</th>
                        </tr>
                    </thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        );

        return (
            <div id='panel_content'>
                <table className='bundle-meta table'>
                    <tbody>
                        <tr>
                            <th>uuid</th>
                            <td>{worksheet.uuid}</td>
                        </tr>
                        <tr>
                            <th>name</th>
                            <td>
                                <WorksheetEditableField
                                    canEdit={true}
                                    fieldName='name'
                                    value={worksheet.name}
                                    uuid={worksheet.uuid}
                                    onChange={this.props.bundleMetadataChanged}
                                />
                            </td>
                        </tr>
                        <tr>
                            <th>title</th>
                            <td>
                                <WorksheetEditableField
                                    canEdit={true}
                                    fieldName='title'
                                    value={worksheet.title}
                                    uuid={worksheet.uuid}
                                    onChange={this.props.bundleMetadataChanged}
                                />
                            </td>
                        </tr>
                        <tr>
                            <th>owner</th>
                            <td>
                                {worksheet.owner_name == null
                                    ? '<anonymous>'
                                    : worksheet.owner_name}
                            </td>
                        </tr>
                        <tr>
                            <th>permissions</th>
                            <td>{renderPermissions(worksheet)}</td>
                        </tr>
                    </tbody>
                </table>
                {bundles_html}
            </div>
        );
    }
}

export default WorksheetSidePanel;
