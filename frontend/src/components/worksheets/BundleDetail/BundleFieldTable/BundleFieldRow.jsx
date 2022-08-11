import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';
import HelpOutlineOutlinedIcon from '@material-ui/icons/HelpOutlineOutlined';
import { BundleEditableField } from '../../../EditableField';
import Copy from '../../../Copy';

/**
 * This component renders bundle fields as table rows.
 * It should be rendered inside of a BundleFieldTable component.
 *
 * It accepts field objects with the following shape:
 *     {
 *         name:        <field_name>,
 *         value:       <field_value>,
 *         description: <field_description>,
 *         editable:    <field_is_editable>,
 *         type:        <field_type>,
 *         bundle_uuid: <bundle_uuid>,
 *     }
 *
 * It accepts `value` and `description` props that override field object data.
 */
class BundleFieldRow extends React.Component {
    constructor(props) {
        super(props);
    }

    checkHideRow() {
        const field = this.props.field || {};
        const value = this.props.value || field?.value;
        if (!field.editable) {
            if (!value) {
                return true;
            }
            if (field.type === 'list' && (!value.length || !value[0])) {
                return true;
            }
        }
        return false;
    }

    render() {
        const { allowCopy, classes, onChange, noWrap } = this.props;
        const hideRow = this.checkHideRow();
        const field = this.props.field || {};
        const name = field.name;
        const dataType = field.type;
        const canEdit = field.editable;
        const uuid = field.bundle_uuid;

        // allow props to override field values
        const label = this.props.label || field.name;
        const description = this.props.description || field.description;
        const value = this.props.value || field.value;
        const copyValue = this.props.copyValue || value;

        if (hideRow) {
            return null;
        }

        return (
            <tr>
                <td className={classes.td}>
                    <Typography variant='subtitle2' inline>
                        {label}
                    </Typography>
                    {description && (
                        <Tooltip title={description} classes={{ tooltip: classes.tooltip }}>
                            <span className={classes.tooltipIcon}>
                                <HelpOutlineOutlinedIcon
                                    fontSize='inherit'
                                    style={{ verticalAlign: 'sub' }}
                                />
                            </span>
                        </Tooltip>
                    )}
                </td>
                <td className={classes.td}>
                    {canEdit ? (
                        <div className={classes.wrappableText}>
                            <BundleEditableField
                                dataType={dataType}
                                fieldName={name}
                                uuid={uuid}
                                value={value}
                                onChange={onChange}
                                canEdit
                            />
                        </div>
                    ) : (
                        <div className={classes.dataContainer}>
                            <Typography noWrap={noWrap}>{value}</Typography>
                            {allowCopy && <Copy message={`${label} Copied!`} text={copyValue} />}
                        </div>
                    )}
                </td>
            </tr>
        );
    }
}

const styles = (theme) => ({
    tooltip: {
        fontSize: 14,
    },
    tooltipIcon: {
        display: 'inline-block',
        verticalAlign: 'inherit',
        color: theme.color.grey.dark,
        paddingLeft: theme.spacing.unit,
        paddingRight: theme.spacing.unit,
        fontSize: 'small',
    },
    wrappableText: {
        flexWrap: 'wrap',
        flexShrink: 1,
    },
    dataContainer: {
        display: 'flex',
        verticalAlign: 'center',
    },
    td: {
        width: '50%',
        verticalAlign: 'top',
        overflowWrap: 'anywhere',
        paddingBottom: 5,
        fontSize: 14,
    },
});

export default withStyles(styles)(BundleFieldRow);
