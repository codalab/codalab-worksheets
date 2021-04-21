// @flow
import * as React from 'react';
import {useEffect, useState} from 'react';
import * as $ from 'jquery';
// import Drawer from '@material-ui/core/Drawer';
import { JsonApiDataStore } from 'jsonapi-datastore';

import ConfigurationPanel from '../ConfigPanel';
import MainContent from './MainContent';
import BundleDetailSideBar from './BundleDetailSideBar';
import BundleActions from './BundleActions';
import {findDOMNode} from "react-dom";
import useSWR from "swr";

const BundleDetail = ({ uuid,
                          // Callback on metadata change.
                          bundleMetadataChanged,
    onClose,
    onOpen,
                          onUpdate,
                          rerunItem, showNewRerun,
                          showDetail, handleDetailClick,
                          editPermission}) =>{
    const [errorMessages, setErrorMessages] = useState([]);
    const [bundleInfo, setBundleInfo] = useState(null);
    const [fileContents, setFileContents] = useState(null);
    const [stdout, setStdout] = useState(null);
    const [stderr, setStderr] = useState(null);
    const [prevUuid, setPrevUuid] = useState(uuid);
    const [open, setOpen] = useState(true);

    useEffect(() => {
        if (uuid !== prevUuid) {
           setPrevUuid(uuid);
           setErrorMessages([]);
        }
    }, [ uuid ])

    // If info is not available yet, fetch
    // If bundle is in a state that is possible to transition to a different state, fetch data
    // we have ignored ready|failed|killed states here
    const refreshInterval =  !bundleInfo || bundleInfo.state.match("uploading|created|staged|making|starting|preparing|running|finalizing|worker_offline") ? 4000: 0;

    useEffect(() => {
        const timer = setInterval(() => {
            if(onOpen){
                onOpen();
            }
        }, 4000);
        return () => clearInterval(timer);
    }, []);

    /**
     * Return a Promise to fetch the summary of the given file.
     * @param uuid  uuid of bundle
     * @param path  path within the bundle
     * @return  jQuery Deferred object
     */
    const fetchFileSummary = (uuid, path)=> {
        return $.ajax({
            type: 'GET',
            url: '/rest/bundles/' + uuid + '/contents/blob' + path,
            data: {
                head: 50,
                tail: 50,
                truncation_text: '\n... [truncated] ...\n\n',
            },
            dataType: 'text',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
        });
    }

    const fetcherMetadata = (url) =>
        fetch(url, {
            type: 'GET',
            url: url,
            dataType: 'json',
        })
            .then((r) => {
                return r.json();
            })
            .catch((error) => {
                setBundleInfo(null);
                setFileContents(null);
                setStderr(null);
                setStdout(null);
                setErrorMessages(errorMessages=>errorMessages.concat([xhr.responseText]))
            });

    const urlMetadata =
        '/rest/bundles/' + uuid+  "?" + new URLSearchParams({include_display_metadata: 1,include: 'owner,group_permissions,host_worksheets'}).toString()

    const{dataMetadata,errorMetadata,mutateMetadata} = useSWR(urlMetadata, fetcherMetadata, {
        revalidateOnMount: true,
        refreshInterval:refreshInterval,
        onSuccess: (response, key, config) => {
            // Normalize JSON API doc into simpler object
            const bundleInfo = new JsonApiDataStore().sync(response);
            bundleInfo.editableMetadataFields = response.data.meta.editable_metadata_keys;
            bundleInfo.metadataType = response.data.meta.metadata_type;
            setBundleInfo( bundleInfo );
        }
    });

    const fetcherContents = (url) =>
        $.ajax( {
            type: 'GET',
            url: url,
            dataType: 'json',
        }).fail(
                function(xhr, status, err) {
                // 404 Not Found errors are normal if contents aren't available yet, so ignore them
                if (xhr.status !== 404) {
                    setBundleInfo(null);
                    setFileContents(null);
                    setStderr(null);
                    setStdout(null);
                    setErrorMessages(errorMessages=>errorMessages.concat([xhr.responseText]));
                } else {
                    // If contents aren't available yet, then also clear stdout and stderr.
                    setFileContents(null);
                    setStderr(null);
                    setStdout(null);
                }
            }
            );

    const urlContents =
        '/rest/bundles/' + uuid + '/contents/info/' +  "?" + new URLSearchParams({depth: 1});

    useSWR(urlContents, fetcherContents, {
        revalidateOnMount: true,
        refreshInterval:refreshInterval,
        onSuccess: (response, key, config) => {
            const info = response.data;
            if (!info) return;
            if (info.type === 'file' || info.type === 'link') {
                return fetchFileSummary(uuid, '/').then(function(blob) {
                    setFileContents(blob);
                    setStderr(null);
                    setStdout(null);
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
                                fetchFileSummary(uuid, '/' + name).then(
                                    function(blob) {
                                        stateUpdate[name] = blob;
                                    },
                                ),
                            );
                        } else {
                            stateUpdate[name] = null;
                        }
                    },
                );
               Promise.all(fetchRequests).then(r => {setFileContents(stateUpdate['fileContents'])
                if('stdout' in stateUpdate){setStdout(stateUpdate['stdout'])}
                if('stderr' in stateUpdate){setStderr(stateUpdate['stderr'])}} )
            }
        },
    });

   const  scrollToNewlyOpenedDetail=(node)=>{
        // Only scroll to the bundle detail when it is opened
        if (node && open) {
            findDOMNode(node).scrollIntoView({block:'center'});
            // Avoid undesirable scroll
            setOpen(false);
        }
    }

    if (!bundleInfo){
        return <div></div>
    }
    if (bundleInfo.bundle_type === 'private') {
        return <div>Detail not available for this bundle</div>
    }

    return (
        <ConfigurationPanel
            //  The ref is created only once, and that this is the only way to properly create the ref before componentDidMount().
            ref={(node) => scrollToNewlyOpenedDetail(node)}
            buttons={ <BundleActions
                showNewRerun={showNewRerun}
                showDetail={showDetail}
                handleDetailClick={handleDetailClick}
                bundleInfo={ bundleInfo }
                rerunItem={ rerunItem }
                onComplete={ bundleMetadataChanged }
                editPermission={editPermission} /> }
            sidebar={ <BundleDetailSideBar bundleInfo={ bundleInfo } onUpdate={ onUpdate } onMetaDataChange={ mutateMetadata } /> }
        >
            <MainContent
                bundleInfo={ bundleInfo }
                stdout={ stdout }
                stderr={ stderr }
                fileContents={ fileContents }
            />
        </ConfigurationPanel>
    );


}


export default BundleDetail;