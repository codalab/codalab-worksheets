import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import { shorten_uuid } from '../../../util/worksheet_utils';

/**
 * This component renders bundle dependencies in an MUI table.
 */
class BundleDependencies extends React.PureComponent {
    constructor(props) {
        super(props);
    }

    render() {
        const { bundleInfo } = this.props;
        const dependencies = bundleInfo.dependencies;

        if (!dependencies.length) {
            return <div>None</div>;
        }

        const dependencies_table = [];
        dependencies.forEach((dep, i) => {
            let dep_bundle_url = '/bundles/' + dep.parent_uuid;
            dependencies_table.push(
                <TableRow key={dep.parent_uuid + i}>
                    <TableCell>
                        {dep.child_path}
                        <br /> &rarr; {dep.parent_name}(
                        <a href={dep_bundle_url} target='_blank'>
                            {shorten_uuid(dep.parent_uuid)}
                        </a>
                        ){dep.parent_path ? '/' + dep.parent_path : ''}
                    </TableCell>
                </TableRow>,
            );
        });

        return (
            <Table>
                <TableBody>{dependencies_table}</TableBody>
            </Table>
        );
    }
}

export default BundleDependencies;
