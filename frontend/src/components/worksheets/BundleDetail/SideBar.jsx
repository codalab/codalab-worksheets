// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import CopyIcon from '@material-ui/icons/FileCopy';
import Tooltip from '@material-ui/core/Tooltip';
import { CopyToClipboard } from 'react-copy-to-clipboard';
import { BundleEditableField } from '../../EditableField';
import PermissionDialog from '../PermissionDialog';
import { renderFormat, shorten_uuid } from '../../../util/worksheet_utils';
import { ConfigLabel } from '../ConfigPanel';
import { renderPermissions } from '../../../util/worksheet_utils';
import Popover from '@material-ui/core/Popover';


class Dependency extends React.PureComponent<
    {
        bundleInfo: {},
        classes: {},
    }
> {

  render() {
    const { bundleInfo } = this.props;
    let dependencies_table = [];
    if (!bundleInfo.dependencies || bundleInfo.dependencies.length == 0) return <div />;

    bundleInfo.dependencies.forEach((dep, i) => {
        let dep_bundle_url = '/bundles/' + dep.parent_uuid;
        dependencies_table.push(
            <TableRow key={ dep.parent_uuid + i }>
                <TableCell>
                    { dep.child_path }
                        &rarr; { dep.parent_name }(
                        <a href={ dep_bundle_url } target="_blank">
                          { shorten_uuid(dep.parent_uuid) }
                        </a>)
                        { dep.parent_path ? '/' + dep.parent_path : '' }
                </TableCell>
            </TableRow>
        );
    });

    return (
        <div>
            <Table>
                <TableBody>{ dependencies_table }</TableBody>
            </Table>
        </div>
    );
  }
}

/**
 * Content to display in sidebar of a Bundle Detail expansion panel.
 */
class SideBar extends React.Component{
    state= {
        showPermisson: false,
    }
  
    render() {
        const { bundleInfo, classes, onUpdate } = this.props;
        const { metadata, editableMetadataFields=[], metadataType } = bundleInfo;
        const hasEditPermission = bundleInfo.permission > 1;
        const bundleState = (bundleInfo.state == 'running' &&
							bundleInfo.metadata.run_status != 'Running')
					? bundleInfo.metadata.run_status
					: bundleInfo.state;
        const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
        const bundleStateClass = 'bundle-state state-' + (bundleInfo.state || 'ready');
        const isRunBundle = bundleInfo.bundle_type === 'run';
        const stateSpecClass = bundleInfo.state === 'failed'
            ? 'failedState'
            : (bundleInfo.state === 'ready' ? 'readyState' : 'otherState');

        return (
            <div>
                { metadata.failure_message && <Typography variant="body1">{ metadata.failure_message }</Typography> }
                {/** ----------------------------------------------------------------------------------------------- */}
                <ConfigLabel label="Name" />
                <div style={{ maxWidth: 300, flexWrap: 'wrap', flexShrink: 1}}>
                    <BundleEditableField
                        dataType={ metadataType.name }
                        fieldName="name"
                        uuid={ bundleInfo.uuid }
                        value={ metadata.name }
                        canEdit={ hasEditPermission && editableMetadataFields.includes("name") }
                        onChange={ (name) => onUpdate({ name }) }
                    />
                </div>
                {/** ----------------------------------------------------------------------------------------------- */}
                <ConfigLabel label="Description" />
                <div style={{ maxWidth: 300, flexWrap: 'wrap', flexShrink: 1}}>
                    <BundleEditableField
                        dataType={ metadataType.description }
                        fieldName="description"
                        uuid={ bundleInfo.uuid }
                        value={ metadata.description }
                        canEdit={ hasEditPermission && editableMetadataFields.includes("description") }
                        onChange={ (description) => onUpdate({ description }) }
                    />
                </div>
                <div>
                    <ConfigLabel label="Status" inline={true}/>
                    <div className={ `${ classes.stateBox } ${ classes[stateSpecClass] }`} style={{ display: 'inline' }}>
                        { bundleState }
                    </div>
                </div>
                { isRunBundle &&
                        <Grid item xs={12}>  
                            <CopyToClipboard
                                text={ bundleInfo.command }
                            >
                                <div>
                                <div style={{ display: 'inline'}}>
                                <ConfigLabel label="Command" inline={true}/>
                                    <div className={classes.copyBox} style={{ display: 'inline' }}>
                                        Copy
                                    </div>
                                </div>
                                
                                <div style={{ maxWidth: 300, flexWrap: 'wrap', flexShrink: 1}}>

                                
                                    {bundleInfo.command}
                                </div>
                                </div>
                            </CopyToClipboard>
                        </Grid>
                    }
                
                {/** ----------------------------------------------------------------------------------------------- */}
                <div>
                    <ConfigLabel label="Owner:" inline={true}/>
                    <div className={classes.dataText}>
                        { bundleInfo.owner.user_name }
                    </div>
                </div>
                <div>
                    <ConfigLabel label="Created:" inline={true}/>
                    <div className={classes.dataText}>
                        {renderFormat(metadata.created, metadataType.created)}
                    </div>
                </div>
                {metadata.data_size 
                    ?   <div>
                            <ConfigLabel label="Size:" inline={true}/>
                            <div className={classes.dataText}>                    
                                {renderFormat(metadata.data_size, metadataType.data_size) }
                            </div>
                        </div>
                    :null
                }
                <ConfigLabel label="Dependencies:" />
                { isRunBundle? <Dependency bundleInfo={ bundleInfo }/> : <div>None</div>}
                <div>
                    <ConfigLabel label="Permissions:" inline={true}/>
                    <div
                        onClick={() => { this.setState({ showPermisson: !this.state.showPermisson}) }}
                        className={classes.permissions}
                    >
                        {renderPermissions(bundleInfo)}
                    </div>
                    {this.state.showPermisson
                    ?   <div>
                            <PermissionDialog
                                uuid={bundleInfo.uuid}
                                permission_spec={bundleInfo.permission_spec}
                                group_permissions={bundleInfo.group_permissions}
                                onChange={this.props.onMetaDataChange}
                                wperm
                            />
                        </div>
                    :   null
                    }
                </div>
                <ConfigLabel label="Attached to these Worksheets:" />
                {
                    bundleInfo.host_worksheets.length > 0
                    ?   bundleInfo.host_worksheets.map((worksheet) =>
                            <div
                                key={ worksheet.uuid }
                            >
                                <a
                                    href={ `/worksheets/${ worksheet.uuid }`}
                                    className={ classes.uuidLink }
                                    target="_blank"
                                >
                                    { worksheet.name }
                                </a>
                            </div>
                        )
                    : <div>None</div>
                }
                {/** ----------------------------------------------------------------------------------------------- */}
                
                <div>
                    <a
                        href={ `/bundles/${ bundleInfo.uuid }` }
                        className={ classes.uuidLink }
                        target="_blank"
                    >
                        More details
                    </a>
                </div>
            </div>
        );
    }
}

const styles = (theme) => ({
    section: {
        marginTop: 8,
    },
    row: {
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
    },
    uuidLink: {
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        }
    },
    spacer: {
        marginTop: theme.spacing.larger,
    },
    stateBox: {
        color: 'white',
        fontSize: '12px',
        width: `50px`,
        textAlign: 'center',
        paddingLeft: 3,
        paddingRight: 3,
        marginLeft: 3,
        border: '1px solid',
        borderRadius: '5px!important'
    },
    copyBox: {
        color: 'white',
        fontSize: '12px',
        width: `50px`,
        textAlign: 'center',
        paddingLeft: 3,
        paddingRight: 3,
        marginLeft: 3,
        border: '1px solid',
        borderRadius: '5px!important',
        backgroundColor: 'black',
        cursor: 'copy',
    },
    readyState: {
        backgroundColor: theme.color.green.base,
    },
    failedState: {
        backgroundColor: theme.color.red.base,
    },
    otherState: {
        backgroundColor: theme.color.yellow.base,
    },
    command: {
		backgroundColor: '#333',
		color: 'white',
		fontFamily: 'monospace',
		padding: theme.spacing.large,
        borderRadius: theme.spacing.unit,
        wordWrap: 'break-all',
        maxWidth: 300,
        flexWrap: 'wrap',
    },
    permissions: {
        cursor: 'pointer',
        '&:hover': {
            backgroundColor: theme.color.primary,
        },
    },
    dataText: {
        display:'inline', 
        fontSize: 14, 
        verticalAlign: 'middle', 
        paddingLeft: 2,
    },
});

export default withStyles(styles)(SideBar);
