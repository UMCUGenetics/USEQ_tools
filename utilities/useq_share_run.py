#!/env/bin/python
"""
USEQ Share Run Module

This module handles sharing of sequencing run data via Nextcloud.
Supports both Illumina and Nanopore sequencing data.
"""

import csv
import datetime
import glob
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any

from genologics.entities import Project, Researcher, Sample
from genologics.lims import Lims
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from texttable import Texttable

from config import Config
from modules.useq_mail import send_mail
from modules.useq_nextcloud import NextcloudUtil
from modules.useq_template import render_template
from utilities.useq_sample_report import get_sample_measurements
from modules.useq_illumina_parsers import parse_sample_sheet
from modules.useq_ui import query_yes_no

class DatabaseManager:
    """
    Manages database connections and operations.

    Attributes:
        session (Session): The active SQLAlchemy database session.
        Run: The automapped Portal DB 'run' table class.
        IlluminaSequencingStats: The 'illumina_sequencing_stats' table class.
        NanoporeSequencingStats: The 'nanopore_sequencing_stats' table class.
    """

    def __init__(self):
        self.session = None
        self.Run = None
        self.IlluminaSequencingStats = None
        self.NanoporeSequencingStats = None

    def create_session(self) -> Tuple:
        """
        Create and return database session and model classes.

        Returns:
            Tuple: A tuple containing (session, Run, IlluminaStats, NanoporeStats).
        """
        Base = automap_base()
        ssl_args = {'ssl_ca': Config.SSL_CERT}
        engine = create_engine(
            Config.PORTAL_DB_URI,
            connect_args=ssl_args,
            pool_pre_ping=True,
            pool_recycle=21600
        )

        Base.prepare(engine, reflect=True)
        self.Run = Base.classes.run
        self.IlluminaSequencingStats = Base.classes.illumina_sequencing_stats
        self.NanoporeSequencingStats = Base.classes.nanopore_sequencing_stats
        self.session = Session(engine)

        return self.session, self.Run, self.IlluminaSequencingStats, self.NanoporeSequencingStats


class StatsParser:
    """Utilities for parsing sequencing statistics from various file formats."""

    @staticmethod
    def parse_samplesheet(run_dir: Path, project_id: str) -> Optional[Dict]:
        """Extracts lane information for a project from a SampleSheet CSV.

        Args:
            run_dir (Path): The root directory of the sequencing run.
            project_id (str): The LIMS project ID to search for.

        Returns:
            Optional[List[int]]: A list of unique lane numbers associated with
                the project. Returns None if the SampleSheet is missing or
                lanes cannot be determined.
        Raises:
            IOError: If samplesheet_file could not be read.
            ValueError: If Sample_Project is not in the samplesheet file.
        """
        samplesheet_file = run_dir / "Conversion" / project_id / "Demux-check" / f"SampleSheet-{project_id}" / "Reports" / "SampleSheet.csv" #Demux check samplesheet should always be there

        pid_lanes = set()
        if not samplesheet_file.is_file():
            print(f"Warning: Could not find {samplesheet_file} file.")
            return None

        try:
            samplesheet_info = parse_sample_sheet(samplesheet_file)

            for sample in samplesheet_info['samples']:

                if not 'Lane' in samplesheet_info['header']:
                    print(f"Warning: No lane found in {samplesheet_file} file, assuming {project_id} was sequenced on a full flowcell.")
                    return None
                lane = int(sample[samplesheet_info['header'].index('Lane')])
                pid = sample[samplesheet_info['header'].index('Sample_Project')]
                if pid == project_id:
                    pid_lanes.add(lane)
        except (IOError, ValueError) as e:
            print(f"Error parsing SampleSheet: {e}")
            return None

        return list(pid_lanes)
    @staticmethod
    def parse_summary_stats(run_dir: Path, pid_lanes: List) -> Optional[Dict]:
        """Parses Illumina summary statistics from the run summary CSV.

        Args:
            run_dir (Path): The root directory of the sequencing run.
            pid_lanes (List[int]): Lanes to include in the statistics.

        Returns:
            Optional[Dict[str, float]]: Dictionary of calculated stats including yield, reads, cluster density, and quality metrics.

        Raises:
            IOError: If summary_stats_file could not be read.
            ValueError: If one or more of the expected stat fields is not present.
        """
        run_name = run_dir.name
        summary_stats_file = run_dir / "Conversion" / "Reports" / f"{run_name}_summary.csv"

        if not summary_stats_file.is_file():
            print(f'Warning: Could not find {summary_stats_file} file.')
            return None

        tmp = {}
        try:
            with open(summary_stats_file, 'r') as sumcsv:
                lines = sumcsv.readlines()
                line_nr = 0

                while line_nr < len(lines):
                    line = lines[line_nr].rstrip()
                    if not line:
                        line_nr += 1
                        continue

                    if line.startswith('Level'):
                        header = [x.rstrip() for x in line.split(",")]
                        line_nr += 1

                        while line_nr < len(lines):
                            sub_line = lines[line_nr].rstrip()
                            if not sub_line:
                                break

                            cols = [x.rstrip() for x in sub_line.split(",")]
                            # stats['summary'].append(dict(zip(header, cols)))
                            line_nr += 1

                            if sub_line.startswith('Total'):
                                break

                    elif line.startswith('Read'):
                        read = line.rstrip()
                        line_nr += 1

                        if line_nr < len(lines):
                            header_line = lines[line_nr].rstrip()
                            header = [x.rstrip() for x in header_line.split(",")]
                            line_nr += 1

                            if read not in tmp:
                                tmp[read] = {}

                            while line_nr < len(lines):
                                sub_line = lines[line_nr].rstrip()
                                if sub_line.startswith('Read') or sub_line.startswith('Extracted'):
                                    break

                                cols = [x.rstrip().split(" ")[0] for x in sub_line.split(",")]
                                if len(cols) > 1 and cols[1] == "-":  # lane summary line
                                    col_dict = dict(zip(header, cols))
                                    if int(col_dict['Lane']) in pid_lanes or not pid_lanes:
                                        tmp[read][col_dict['Lane']] = col_dict

                                line_nr += 1
                    else:
                        line_nr += 1


            stats = {
                'yield_r1': 0, 'yield_r2': 0, 'reads': 0, 'cluster_density': 0,
                'perc_q30_r1': 0, 'perc_q30_r2': 0, 'perc_occupied': 0, 'phix_aligned': 0
            }
            # Process temporary data into final format
            nr_lanes = 0
            for read in tmp:
                if '(I)' in read:  # Skip index reads
                    continue
                read_nr = 2 if not '1' in read else 1
                for lane in tmp[read]:

                    stats[f'yield_r{read_nr}'] += float(tmp[read][lane]['Yield'])
                    stats[f'perc_q30_r{read_nr}'] += float(tmp[read][lane]['%>=Q30'])
                    if read_nr == 1:
                        nr_lanes +=1
                        stats['reads'] += float(tmp[read][lane]['Reads PF'])
                        stats['cluster_density'] += float(tmp[read][lane]['Density'])
                        stats['perc_occupied'] += float(tmp[read][lane]['% Occupied'])
                        stats['phix_aligned'] += float(tmp[read][lane]['Aligned'])

                stats[f'perc_q30_r{read_nr}'] = round((stats[f'perc_q30_r{read_nr}'] / nr_lanes), 2)
            stats['cluster_density'] = round((stats['cluster_density'] / nr_lanes), 2)
            stats['perc_occupied'] = round((stats['perc_occupied'] / nr_lanes), 2)
            stats['phix_aligned'] = round((stats['phix_aligned'] / nr_lanes), 2)

        except (IOError, ValueError) as e:
            print(f"Error parsing summary stats: {e}")
            return None


        return stats

    @staticmethod
    def parse_conversion_stats(lims: Lims, run_dir: Path, project_id: str) -> Optional[Dict]:
        """
        Parse Illumina conversion statistics from CSV files.

        Args:
            lims (Lims): LIMS instance.
            run_dir (Path): The root directory of the sequencing run.
            project_id (str): LIMS Project ID

        Returns:
            Optional[Dict[]: Dictionary of calculated stats including total_reads, total_mean_qual, total_q30, avg_quality_r1, avg_quality_r2 and
            sample specific info:
            -Index
            -Lane(s)
            -# Reads
            -# Perfect Index Reads
            -# One Mismatch Index Reads

        Raises:
            IOError: If demux_stats_file or qual_metrics_file could not be read.
            ValueError: If one or more of the expected stat fields is not present.
        """


        demux_stats_file = run_dir / "Conversion" / project_id / "Reports" / "Demultiplex_Stats.csv"
        qual_metrics_file = run_dir / "Conversion" / project_id / "Reports" / "Quality_Metrics.csv"

        for stats_file in [demux_stats_file, qual_metrics_file]:
            if not stats_file.is_file():
                print(f'Warning: Could not find {stats_file} file.')
                return None

        try:
            samples = lims.get_samples(projectlimsid=project_id)
            sample_names = [sample.name for sample in samples]

            stats = {
                'total_reads': 0, 'total_mean_qual': 0, 'total_q30': 0,
                'avg_quality_r1': 0, 'avg_quality_r2': 0, 'samples': []
            }

            # Parse demultiplexing stats
            samples_data = {}
            with open(demux_stats_file, 'r') as demux_file:
                csv_reader = csv.DictReader(demux_file)
                for row in csv_reader:
                    stats['total_reads'] += float(row['# Reads'])

                    sample_id = row['SampleID']
                    if sample_id not in samples_data:
                        samples_data[sample_id] = {
                            'Index': None, '# Reads': 0, '# Perfect Index Reads': 0,
                            '# One Mismatch Index Reads': 0, 'Lane': []
                        }

                    samples_data[sample_id]['Index'] = row['Index']
                    samples_data[sample_id]['Lane'].append(int(row['Lane']))
                    samples_data[sample_id]['# Reads'] += int(row['# Reads'])
                    samples_data[sample_id]['# Perfect Index Reads'] += int(row['# Perfect Index Reads'])
                    samples_data[sample_id]['# One Mismatch Index Reads'] += int(row['# One Mismatch Index Reads'])

            # Parse quality metrics
            qual_metrics_rows = 0
            read1_rows = 0

            with open(qual_metrics_file, 'r') as qual_file:
                csv_reader = csv.DictReader(qual_file)
                for row in csv_reader:
                    qual_metrics_rows += 1
                    sample_id = row['SampleID']
                    read_num = row["ReadNumber"]

                    mqs_key = f'Read {read_num} Mean Quality Score (PF)'
                    q30_key = f'Read {read_num} % Q30'

                    if mqs_key not in samples_data[sample_id]:
                        samples_data[sample_id][mqs_key] = 0
                    if q30_key not in samples_data[sample_id]:
                        samples_data[sample_id][q30_key] = 0

                    if read_num == '1':
                        read1_rows += 1
                        stats['avg_quality_r1'] += float(row['Mean Quality Score (PF)'])
                    elif read_num == '2':
                        stats['avg_quality_r2'] += float(row['Mean Quality Score (PF)'])

                    samples_data[sample_id][mqs_key] += float(row['Mean Quality Score (PF)'])
                    samples_data[sample_id][q30_key] += float(row['% Q30'])
                    stats['total_q30'] += float(row['% Q30'])
                    stats['total_mean_qual'] += float(row['Mean Quality Score (PF)'])

            # Calculate averages
            if qual_metrics_rows > 0:
                stats['total_q30'] = (stats['total_q30'] / qual_metrics_rows) * 100
                stats['total_mean_qual'] = stats['total_mean_qual'] / qual_metrics_rows

            if read1_rows > 0:
                stats['avg_quality_r1'] = round(stats['avg_quality_r1'] / read1_rows, 2)
                stats['avg_quality_r2'] = round(stats['avg_quality_r2'] / read1_rows, 2)

            # Process sample data
            for sample_id, sample_data in samples_data.items():
                if sample_id not in sample_names:
                    continue

                sample = {'SampleID': sample_id, 'Index': sample_data['Index']}
                lane_count = len(sample_data['Lane'])

                for read_num in ['1', '2', 'I1', 'I2']:
                    mqs_key = f'Read {read_num} Mean Quality Score (PF)'
                    q30_key = f'Read {read_num} % Q30'

                    if mqs_key in sample_data:
                        sample[mqs_key] = sample_data[mqs_key] / lane_count
                    if q30_key in sample_data:
                        sample[q30_key] = (sample_data[q30_key] / lane_count) * 100

                for key in ['# Reads', '# Perfect Index Reads', '# One Mismatch Index Reads']:
                    sample[key] = sample_data[key]

                stats['samples'].append(sample)

        except (IOError, ValueError) as e:
            print(f"Error parsing conversion stats: {e}")
            return None

        return stats


class RunDetailsRetriever:
    """Retrieves run details for different sequencing platforms."""

    @staticmethod
    def get_nanopore_run_details(lims: Lims, project_id: str) -> Optional[Dict]:
        """
        Get Nanopore run details from summary files.

        Args:
            lims (Lims): LIMS instance.
            project_id (str): LIMS Project ID

        Returns:
            Optional[Dict[str, Any]: Dictionary of latest nanopore run details corresponding to project_id, contains:
            -flowcell_id
            -run_dir
            -stats_file
            -date

        Raises:
            IOError: If summary_file could not be read.
            ValueError: If 'started' or 'flow_cell_id' can not be found in summary_file.

        """

        runs = {}
        pattern = f"{Config.HPC_RAW_ROOT}/nanopore/**/final_summary_*txt"

        for summary_file in glob.glob(pattern, recursive=True):
            parent_dir = Path(summary_file).parent

            try:
                with open(summary_file, 'r') as file:
                    summary_data = {}
                    for line in file.readlines():
                        if '=' in line:
                            name, val = line.rstrip().split("=", 1)
                            summary_data[name] = val

                    if 'protocol_group_id' in summary_data and project_id in summary_data['protocol_group_id']:
                        stats_files = {
                            'pdf': glob.glob(f"{parent_dir}/*pdf"),
                            'html': glob.glob(f"{parent_dir}/*html")
                        }

                        run_date = datetime.datetime.strptime(
                            summary_data['started'].split("T")[0], "%Y-%m-%d"
                        )

                        stats_file = None
                        if stats_files['pdf']:
                            stats_file = stats_files['pdf'][0]
                        elif stats_files['html']:
                            stats_file = stats_files['html'][0]

                        runs[run_date] = {
                            'flowcell_id': summary_data['flow_cell_id'],
                            'run_dir': parent_dir,
                            'stats_file': stats_file,
                            'date': run_date
                        }

            except (IOError, ValueError) as e:
                print(f"Error reading {summary_file}: {e}")
                continue

        if not runs:
            return None

        latest_date = max(runs.keys())
        return runs[latest_date]

    @staticmethod
    def get_illumina_run_details(lims: Lims, project: Project, fid: Optional[str]) -> Optional[Tuple]:
        """
        Get Illumina run details from LIMS processes.

        Args:
            lims (Lims): LIMS instance.
            project (Project): LIMS Project object.
            fid: Optional flowcell ID. Overwrites any flowcell ID found while looking through the projects' processes.

        Returns:
            Optional[Tuple[Path, str, str]: Returns a tuple containing (in order):
            -Run directory
            -Flowcell ID
            -Date of sequencing

        """
        runs = {}
        project_processes = lims.get_processes(projectname=project.name)

        for process in project_processes:
            if not process.date_run:
                continue

            flowcell_id = None
            if 'Flow Cell ID' in process.udf:
                flowcell_id = fid if fid else process.udf['Flow Cell ID']
            elif fid:
                flowcell_id = fid
            else:
                continue

            denature_step = process.parent_processes()[0] if process.parent_processes() else None
            phix_control = 0
            loading_conc = 0

            # Extract PhiX and loading concentration
            if denature_step and '% PhiX Control' in denature_step.udf:
                phix_control = denature_step.udf['% PhiX Control']

            # Find matching input artifact for this project
            for io_map in process.input_output_maps:
                input_artifact = io_map[0]['uri']
                if input_artifact.samples and input_artifact.samples[0].project.id == project.id:
                    loading_conc = input_artifact.udf.get('Loading Conc. (pM)', 0)
                    if not phix_control:
                        phix_control = input_artifact.udf.get('% PhiX Control', 0)
                    break

            runs[process.date_run] = {
                'flowcell_id': flowcell_id,
                'date_started': process.date_run,
                'phix_loaded': phix_control,
                'load_conc': loading_conc
            }

        if not runs:
            return None

        # Find most recent run
        run_dates = [datetime.datetime.strptime(ts, "%Y-%m-%d") for ts in runs.keys()]
        latest_date = max(run_dates).strftime("%Y-%m-%d")
        latest_flowcell_id = runs[latest_date]['flowcell_id']

        # Find run directory
        for machine in Config.MACHINE_ALIASES:
            machine_dir = Path(Config.HPC_RAW_ROOT) / machine
            if not machine_dir.exists():
                continue

            for run_dir in machine_dir.glob("*"):
                patterns = [
                    f"_000000000-{latest_flowcell_id}",  # MiSeq
                    f"_{latest_flowcell_id}",            # NextSeq
                    f"A{latest_flowcell_id}",            # HiSeq A
                    f"B{latest_flowcell_id}"             # HiSeq B
                ]

                if any(run_dir.name.endswith(pattern) for pattern in patterns):
                    return run_dir, latest_flowcell_id, runs[latest_date]

        return None


class DataSharer:
    """Handles data sharing operations."""

    def __init__(self, nextcloud_util: NextcloudUtil):
        self.nextcloud_util = nextcloud_util
        self.db_manager = DatabaseManager()
        self.stats_parser = StatsParser()
        self.run_retriever = RunDetailsRetriever()

    def _send_text(self, researcher: Researcher, project_id: str, password: str):
        """
        Internal function to send an SMS notification to researcher.

        Args:

            researcher (Researcher): LIMS Researcher object.
            project_id (str): LIMS project ID.
            password (str): Password to share with researcher.

        Returns:
            None

        """

        sms_command = (
            f"ssh usfuser@{Config.SMS_SERVER} "
            f"\"sendsms.py -m 'Dear_{researcher.username},_"
            f"A_link_for_{project_id}_was_send_to_{researcher.email}._"
            f"{password}_is_needed_to_unlock_the_link._Regards,_USEQ' "
            f"-n {researcher.phone}\""
        )
        os.system(sms_command)

    def _create_archive(self, source_dir: Path, archive_path: Path) -> bool:
        """
        Internal function to create a tar archive of a directory.

        Args:
            source_dir (Path): Path of tar target directory
            archive_path (Path): Where to create tar file

        Returns:
            True if tar file was succesfully created, False otherwise.


        """
        archive_command = f"cd {source_dir.parent} && tar -chf {archive_path} {source_dir.name}"
        exit_code = os.system(archive_command)

        if exit_code == 0:
            done_file = Path(f"{archive_path}.done")
            done_file.touch()
            return True
        else:
            print(f"Error: Failed creating {archive_path}")
            return False

    def _upload_to_nextcloud(self, local_path: Path, remote_dir: str) -> bool:
        """
        Internal function to upload a file or directory to Nextcloud.

        Args:
            local_path (Path): Path of file or directory to upload.
            remote_dir (str): Location on Nextcloud to write local_path to.

        Returns:
            True if file or directory was succesfully uploaded to Nextcloud.

        """
        upload_command = (
            f'scp -r {local_path} {Config.NEXTCLOUD_HOST}:'
            f'{Config.NEXTCLOUD_DATA_ROOT}/{remote_dir}'
        )

        exit_code = os.system(upload_command)
        if exit_code != 0:
            print(f"Error: Failed to upload {local_path} to NextCloud")
            return False

        time.sleep(90)  # Wait for upload to complete
        return True

    def _display_texttable(self, rows: List[List]):
        """
        Internal function to display a List of Lists in a userfriendly fashion.

        Args:
            rows (List[List]): A list of lists (basically a matrix)

        """
        table = Texttable(max_width=0)
        # print(rows)
        # for row in rows:
            # print(row)
        table.add_rows(rows)
        print(table.draw())

    def _share_manual_data(self, researcher: Researcher, data_dir: Path):
        """
        Internal function to share a data directory with a researcher.

        Args:
            researcher (Researcher): LIMS Researcher object.
            data_dir (Path): Data directory to share.
        """

        print("Starting manual data sharing")

        archive_path = data_dir / f"{data_dir.name}.tar"
        done_file = Path(f"{archive_path}.done")

        # Create archive if not exists
        if not (archive_path.is_file() and done_file.is_file()):
            print("Creating archive")
            if not self._create_archive(data_dir, archive_path):
                return
        else:
            print(f"Using existing archive")

        # Upload to NextCloud
        print(f"Uploading to NextCloud")
        if not self._upload_to_nextcloud(archive_path, Config.NEXTCLOUD_MANUAL_DIR):
            return

        # Share with researcher
        print(f"Sharing with {researcher.email}")
        share_response = self.nextcloud_util.share(archive_path.name, researcher.email)

        if "ERROR" in share_response:
            print(f"Error sharing: {share_response['ERROR']}")
            return

        # Send notification
        self._send_manual_notification(researcher, data_dir, share_response)

        # Cleanup
        archive_path.unlink(missing_ok=True)
        done_file.unlink(missing_ok=True)


    def share_data_by_user(self, lims: Lims, username: str, directory: str) -> bool:
        """
        Share data with a specific user by username.

        Args:
            lims (Lims): LIMS instance.
            username (str): LIMS username (e.g. u.seq).
            directory (str): Directory to share.

        Returns:
            True if share was successful, False otherwise.

        """
        researchers = lims.get_researchers(username=username)
        data_dir = Path(directory)

        if not researchers or not data_dir.is_dir():
            print(f"Error: Either username {username} or directory {data_dir} does not exist.")
            return False

        researcher = researchers[0]
        if not researcher.phone:
            print(f"Error: {username} has not provided a mobile phone number yet.")
            return False

        # Check if directory contains known samples belonging to username
        possible_samples = {}
        lims_samples = []
        valid_extensions = {'.bam', '.fastq.gz'}
        for file in data_dir.rglob("*"):
            if "".join(file.suffixes) in valid_extensions :
                sample_name = file.name.split("_")[0]
                if sample_name not in possible_samples:
                    possible_samples[sample_name] = []
                possible_samples[sample_name].append(file)

        print (f"Found {len(possible_samples.keys())} possible samples in {data_dir}.")

        if possible_samples:
            print (f"Trying to link samples to existing projectIDs.")
            sample_chunks = chunkify(list(possible_samples.keys()), 100)

            for sample_chunk in sample_chunks:
                lims_samples.extend(lims.get_samples(name=sample_chunk))


            lims_projects = {}
            for sample in lims_samples:
                user = 'NA'
                if not sample.project:
                    continue

                sample_project = sample.project
                try:#There are some older project artifacts without a researcher attached
                    user = sample_project.researcher.username
                except:
                    continue

                user = sample_project.researcher.username

                id = f"{sample_project.id}:{user}"
                if id not in lims_projects:
                    lims_projects[id] = 0
                lims_projects[id] +=1

            table_rows = [
                ['projectID','Username','Nr. Samples']
            ]
            for id in lims_projects:
                projectID, user = id.split(":")
                nr_samples = lims_projects[id]
                table_rows.append([projectID, user, nr_samples])

            print ("\nMatched samples to the following projectIDs & users:")
            self._display_texttable(
                table_rows
            )
        else:
            print (f"Found no valid samples in {dir}. Trying to match {data_dir.name} to an existing LIMS projectID.")
            projectID_matches = re.match("(^\w{3}\d{,5}).*",data_dir.name)
            first_projectID_match = projectID_matches.groups()[0]
            project = None

            try:
                project = Project(lims, id=first_projectID_match)
                id = project.id
                print(f"\nMatch found for {first_projectID_match}")
                self._display_texttable(
                    [
                        ['Project ID','Project Name','Username'],
                        [project.id,project.name, project.researcher.username]
                    ]
                )
            except:
                print (f"No LIMS project found for possible projectID {first_projectID_match}")

        # Display confirmation table
        print("\nData sharing confirmation:")
        self._display_texttable(
            [
                ['Data', 'Directory', 'Client Email'],
                [data_dir.name, str(data_dir), researcher.email]
            ]
        )
        if query_yes_no("Are you sure you want to share this data"):
            self._share_manual_data(researcher, data_dir)
            return True

        return False

    def share_data_by_id(self, lims: Lims, project_id: str, fid: Optional[str], all_dirs_ont: bool) -> bool:
        """
        Share data by project ID.

        Args:
            lims (Lims): LIMS instance.
            project_id (str): LIMS project ID
            fid (Optional[str]): Optional flowcell ID. Overwrites any flowcell ID found while looking through the projects' processes.
            all_dirs_ont (bool): Flag determining packaging logic. If True, packages full data directories (e.g., fastq_pass.tar.gz). If False, attempts to package by individual barcode folders.

        Returns:
            True if share was successful, False otherwise.

        """

        try:
            project = Project(lims, id=project_id)
        except:
            print(f"Error: Project ID {project_id} not found in LIMS!")
            return False

        researcher = project.researcher
        if not researcher.phone:
            print(f'Error: User {researcher.username} has not provided a phone number!')
            return False

        # Check if project exists in portal DB
        session, Run, IlluminaStats, NanoporeStats = self.db_manager.create_session()
        portal_run = session.query(Run).filter_by(run_id=project_id).first()

        if not portal_run:
            print(f'Error: Project ID {project_id} not found in Portal DB!')
            return False

        application = project.udf.get('Application', '')

        if application == 'ONT Sequencing':

            return self._share_nanopore_data(
                lims, project, researcher, project_id, all_dirs_ont, session, NanoporeStats, portal_run
            )
        else:
            return self._share_illumina_data(
                lims, project, researcher, project_id, fid, session, IlluminaStats, portal_run
            )

    def _share_nanopore_data(self, lims: Lims, project: Project, researcher: Researcher, project_id: str, all_dirs_ont: Optional[bool], session: Any, NanoporeStats: Any, portal_run):
        """
        Internal function to handle sharing of Nanopore sequencing data.

        Args:
            lims: LIMS instance.
            project: LIMS Project.
            researcher: LIMS Researcher.
            project_id (str): LIMS project ID.
            all_dirs_ont (bool): Flag determining packaging logic. If True, packages full data directories (e.g., fastq_pass.tar.gz). If False, attempts to package by individual barcode folders.
            session: SQLAlchemy or equivalent database session for tracking stats.
            NanoporeStats: The database model class for Nanopore statistics.
            portal_run: The portal run object/record to be updated.

        Returns:
            True if share was successful, False otherwise.

        Raises:
            OSError: If directory creation or cleanup fails.
        """
        run_info = self.run_retriever.get_nanopore_run_details(lims, project_id)

        if not run_info:
            print('Error: No Nanopore run directory could be found!')
            return False

        run_dir = run_info['run_dir']

        # Display confirmation table
        print("\nNanopore data sharing confirmation:")
        self._display_texttable(
            [
                ['Data', 'Project (ID:Name)', 'Client Email'],
                [run_dir.name, f"{project_id}:{project.name}", researcher.email]
            ]
        )
        if not query_yes_no("Are you sure you want to send this dataset"):
            return False

        # Check if already exists on Nextcloud
        if self.nextcloud_util.check_exists(project_id):
            print(f'Warning: Deleting previous version of {project_id} on Nextcloud')
            self.nextcloud_util.delete(project_id)
            self.nextcloud_util.delete(f'{project_id}.done')

        # Create upload directory and package data
        upload_dir = run_dir / project_id
        upload_dir.mkdir(exist_ok=True)

        file_list = self._package_nanopore_data(run_dir, upload_dir, all_dirs_ont, project_id)

        if not self._upload_to_nextcloud(upload_dir, Config.NEXTCLOUD_RAW_DIR):
            return False

        # Share and send notification
        share_response = self.nextcloud_util.share(project_id, researcher.email)
        if "ERROR" in share_response:
            print(f"Error sharing: {share_response['ERROR']}")
            return False

        self._send_nanopore_notification(
            researcher, project_id, share_response, file_list,
            get_sample_measurements(lims, project_id), run_dir
        )

        # Update database
        self._update_nanopore_stats(session, NanoporeStats, run_info, portal_run, project_id, run_dir)

        # Cleanup
        os.system(f'rm -rf {upload_dir}')
        return True

    def _package_nanopore_data(self, run_dir: Path, upload_dir: Path, all_dirs_ont: bool, project_id: str) -> List[str]:
        """
        Package Nanopore data for sharing.

        Args:
            run_dir (Path): The source directory containing the Nanopore run data.
            upload_dir (Path): The destination directory where the .tar.gz archives will be created.
            all_dirs_ont (bool): Flag determining packaging logic. If True, packages full data directories (e.g., fastq_pass.tar.gz). If False, attempts to package by individual barcode folders.
            project_id (str): LIMS project ID

        Returns:
            A list of filenames (strings) representing all the archives successfully created and moved to the upload directory.

        """
        data_directories = [
            'fast5_pass', 'fast5_fail', 'fastq_pass', 'fastq_fail',
            'bam_pass', 'bam_fail', 'pod5_pass', 'pod5_fail', 'pod5'
        ]

        file_list = []
        available_files_path = run_dir / f'available_files_{project_id}.txt'

        with open(available_files_path, 'w') as available_files:
            # Handle barcode directories or all directories
            fastq_pass_dir = run_dir / 'fastq_pass'

            if fastq_pass_dir.is_dir() and not all_dirs_ont:
                barcode_dirs = [
                    d for d in fastq_pass_dir.iterdir()
                    if d.is_dir() and ('barcode' in d.name or 'unclassified' in d.name)
                ]

                for barcode_dir in barcode_dirs:
                    archive_name = f"{barcode_dir.name}.tar.gz"
                    self._create_barcode_archive(run_dir, upload_dir, barcode_dir, data_directories, archive_name)
                    file_list.append(archive_name)
                    available_files.write(f"{archive_name}\n")
            else:
                for data_dir_name in data_directories:
                    data_dir_path = run_dir / data_dir_name
                    if data_dir_path.is_dir():
                        archive_name = f"{data_dir_name}.tar.gz"
                        self._create_data_archive(run_dir, upload_dir, data_dir_name, archive_name)
                        file_list.append(archive_name)
                        available_files.write(f"{archive_name}\n")

            # Create stats archive
            stats_archive = "stats.tar.gz"
            stats_command = f"cd {run_dir} && tar -czf {upload_dir}/{stats_archive} other_reports/ *.*"
            if os.system(stats_command) == 0:
                file_list.append(stats_archive)
                available_files.write(f"{stats_archive}\n")

        return file_list

    def _create_barcode_archive(self, run_dir: Path, upload_dir: Path, barcode_dir: Path, data_dirs: List[str], archive_name: str):
        """
        Internal function to create an archive for a specific barcode directory.

        Args:
            run_dir (Path): The base directory of the Nanopore run.
            upload_dir (Path): The destination directory where the resulting archive will be stored.
            barcode_dir (Path): The path to the specific barcode directory (used to extract the directory name, e.g., 'barcode01').
            data_dirs (List[str]): A list of subdirectory names to search within (e.g., ['fastq_pass', 'fast5_pass', 'pod5']).
            archive_name (str): The filename for the resulting archive (e.g., 'barcode01.tar.gz').

        Raises:
            RuntimeError: If the `tar` command execution returns a non-zero exit code, indicating a failure in archive creation.
        """
        zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{archive_name}"

        for data_dir_name in data_dirs:
            barcode_path = run_dir / data_dir_name / barcode_dir.name
            if barcode_path.is_dir():
                zip_command += f" {data_dir_name}/{barcode_dir.name}"

        exit_code = os.system(zip_command)
        if exit_code != 0:
            raise RuntimeError(f"Failed to create archive {archive_name}")

    def _create_data_archive(self, run_dir: Path, upload_dir: Path, data_dir_name: str, archive_name: str):
        """
        Internal function to create an archive for data a directory.
        Args:
            run_dir (Path): The base directory of the Nanopore run containing the source data.
            upload_dir (Path): The destination directory where the resulting archive will be saved.
            data_dir_name (str): The name of the directory to be archived (e.g., 'fast5_pass').
            archive_name (str): The desired filename for the resulting .tar.gz file.

        Raises:
            RuntimeError: If the shell command fails (returns a non-zero exit code), likely due to permissions issues or the source directory being missing.

        """
        zip_command = f"cd {run_dir} && tar -czf {upload_dir}/{archive_name} {data_dir_name}"
        exit_code = os.system(zip_command)
        if exit_code != 0:
            raise RuntimeError(f"Failed to create archive {archive_name}")


    def _send_manual_notification(self, researcher: Researcher, data_dir : Path, share_response):
        """
        Send notification for manual data sharing.

        Args:
            researcher (Researcher): LIMS Researcher
            data_dir (Path): The path to the directory being shared; used to extract the directory name for the notification.
            share_response (dict): A dictionary containing Nextcloud share information. Must contain a "SUCCESS" key with a tuple or list of (share_id, password).

        Notes:
            - In production: Sends email to the researcher and an SMS via `_send_text`.
            - In development (`Config.DEVMODE`): Redirects email to a hardcoded dev address and skips the SMS, printing the password instead.
        """
        share_id, password = share_response["SUCCESS"]

        template_data = {
            'name': f"{researcher.first_name} {researcher.last_name}",
            'dir': data_dir.name,
            'nextcloud_host': Config.NEXTCLOUD_HOST,
            'share_id': share_id,
            'phone': researcher.phone
        }

        mail_content = render_template('share_manual_template.html', template_data)
        mail_subject = "USEQ has shared a file with you."

        if Config.DEVMODE:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, 's.w.boymans@umcutrecht.nl')
            print(f"Development mode: Password is {password}")
        else:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, researcher.email)
            self._send_text(researcher, data_dir.name, password)

        print(f'Shared {data_dir} with {researcher.email}')

    def _send_illumina_notification(self, researcher: Researcher, project_id: str, share_response: Dict[str, Any], file_list: List[str],
                                  conversion_stats: Dict[str, Any], sample_measurements: List[Any], samples: List[Sample], available_files_path: Path):
        """
        Internal function to send a notification for Illumina data sharing.

        Args:
            researcher: LIMS Researcher.
            project_id (str): LIMS project ID.
            share_response (Dict[str, Any]): Dictionary containing Nextcloud "SUCCESS" details (share_id, password).
            file_list (List[str]): List of filenames included in the share.
            conversion_stats (Dict[str, Any]): Statistics related to BCL to FASTQ conversion.
            sample_measurements (List): List of sample-specific metrics or QC values.
            samples (List[Sample]): List of LIMS Sample objects used to extract UDF metadata.
            available_files_path (Path): Path to the text file manifest to be attached.

        Notes:
            If `Config.DEVMODE` is True, emails are rerouted to development staff and passwords are printed to the console instead of sent via SMS.

        """
        share_id, password = share_response["SUCCESS"]
        analysis_steps = samples[0].udf.get('Analysis', '').split(',') if samples else []

        template_data = {
            'project_id': project_id,
            'phone': researcher.phone,
            'name': f"{researcher.first_name} {researcher.last_name}",
            'nextcloud_host': Config.NEXTCLOUD_HOST,
            'share_id': share_id,
            'file_list': file_list,
            'conversion_stats': conversion_stats,
            'sample_measurements': sample_measurements,
            'analysis_steps': analysis_steps,
        }

        mail_content = render_template('share_illumina_template.html', template_data)
        mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"
        attachments = {'available_files': str(available_files_path)}

        if Config.DEVMODE:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, 's.w.boymans@umcutrecht.nl', attachments=attachments)
            print(f"Development mode: Password is {password}")
        else:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, researcher.email, attachments=attachments)
            self._send_text(researcher, project_id, password)

        print(f'Shared {project_id} with {researcher.email}')


    def _send_nanopore_notification(self, researcher: Researcher, project_id: str, share_response: Dict[str, Any], file_list: List[str], sample_measurements: List[Any], run_dir: Path):
        """Internal function to send a notification for Nanopore data sharing.

        Args:
            researcher: LIMS Researcher.
            project_id (str): LIMS project ID.
            share_response (Dict[str, Any]): Dictionary containing Nextcloud "SUCCESS" details (share_id, password).
            file_list (List[str]): List of archived filenames (e.g., .tar.gz files).
            sample_measurements (List): List of sample-specific metrics or QC values.
            run_dir (Path): The directory path where the run data and manifest reside.

        """
        share_id, password = share_response["SUCCESS"]

        template_data = {
            'name': f"{researcher.first_name} {researcher.last_name}",
            'project_id': project_id,
            'phone': researcher.phone,
            'nextcloud_host': Config.NEXTCLOUD_HOST,
            'share_id': share_id,
            'file_list': file_list,
            'sample_measurements': sample_measurements
        }

        mail_content = render_template('share_nanopore_template.html', template_data)
        mail_subject = f"USEQ sequencing of sequencing-run ID {project_id} finished"

        attachments = {'available_files': f'{run_dir}/available_files_{project_id}.txt'}

        if Config.DEVMODE:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, 's.w.boymans@umcutrecht.nl', attachments=attachments)
            print(f"Development mode: Password is {password}")
        else:
            send_mail(mail_subject, mail_content, Config.MAIL_SENDER, researcher.email, attachments=attachments)
            self._send_text(researcher, project_id, password)

        print(f'Shared {project_id} with {researcher.email}')

    def _update_nanopore_stats(self, session, NanoporeStats, run_info, portal_run, project_id, run_dir):
        """
        Internal function to update database with Nanopore sequencing statistics.

        Args:
            session: SQLAlchemy or equivalent database session for performing transactions.
            NanoporeStats: The database model class for Nanopore statistics.
            run_info (dict): Dictionary containing run metadata including 'flowcell_id', 'date', and optionally 'stats_file'.
            portal_run: The portal run object/record to which these stats will be linked.
            project_id (str): LIMS project ID
            run_dir (Path): The local directory path where the run data is located.

        Raises:
            OSError: If directory creation or file removal fails.
        """
        flowcell_id = run_info['flowcell_id']

        # Check if stats already exist
        existing_stats = session.query(NanoporeStats).filter_by(flowcell_id=flowcell_id).first()
        if existing_stats:
            print(f'Warning: Stats for {flowcell_id} already exist in portal DB. Skipping.')
            return

        stats_file = run_info.get('stats_file')
        general_stats_filename = ''

        if stats_file:
            # Copy stats file to portal storage
            stats_filename = Path(stats_file).name
            temp_dir = run_dir / flowcell_id
            temp_dir.mkdir(exist_ok=True)

            os.system(f"cp {stats_file} {temp_dir}")

            rsync_command = (
                f"/usr/bin/rsync -rah {temp_dir} "
                f"{Config.PORTAL_USER}@{Config.PORTAL_SERVER}:{Config.PORTAL_STORAGE}/"
            )

            exit_code = os.system(rsync_command)
            if exit_code == 0:
                general_stats_filename = stats_filename
                print(f'Uploaded stats for {project_id} to portal storage.')
            else:
                print(f'Error: Failed to copy stats to {Config.PORTAL_STORAGE}')

            os.system(f"rm -rf {temp_dir}")

        # Create database entry
        nanopore_stats = NanoporeStats(
            general_stats=general_stats_filename,
            date=run_info['date'],
            date_started=run_info['date'],
            date_send=datetime.datetime.now(),
            flowcell_id=flowcell_id,
            run_id=portal_run.id
        )

        session.add(nanopore_stats)
        session.commit()

        if general_stats_filename:
            print(f'Added stats record for {project_id} to portal DB.')
        else:
            print(f'Added minimal record for {project_id} to portal DB (no stats file found).')

    def _share_illumina_data(self, lims, project, researcher, project_id, fid, session, IlluminaStats, portal_run) -> bool:
        """
        Internal function to handle sharing of Illumina sequencing data.
        A short description of what it does:
        1. Locates the Illumina run directory and validates its existence on Nextcloud.
        2. Parses conversion and summary statistics from the run data.
        3. Displays a summary table (Reads, Q30, Quality) for user confirmation.
        4. Generates a manifest of files available on Nextcloud.
        5. Shares the Nextcloud folder and sends a notification email/SMS.
        6. Updates the portal database with detailed Illumina statistics.

        Args:
            lims: LIMS instance
            project: LIMS Project.
            researcher: LIMS Researcher.
            project_id (str): LIMS project ID.
            fid (str): The flowcell ID.
            session: Database session for tracking stats.
            IlluminaStats: The database model class for Illumina statistics.
            portal_run: The portal run object/record to be updated.

        Returns:
            True if the sharing process completed successfully, False otherwise.
        """

        run_details = self.run_retriever.get_illumina_run_details(lims, project, fid)

        if not run_details:
            print('Error: No Illumina run directory could be found!')
            return False

        run_dir, flowcell_id, run_meta = run_details

        # Check if data exists on Nextcloud
        possible_nextcloud_ids = [
            f'{project_id}_{flowcell_id}',
            f'{project_id}_A{flowcell_id}',
            f'{project_id}_B{flowcell_id}'
        ]

        nextcloud_run_id = None
        for run_id in possible_nextcloud_ids:
            if self.nextcloud_util.check_exists(run_id):
                nextcloud_run_id = run_id
                break

        if not nextcloud_run_id:
            print(f'Error: {project_id} was not uploaded to Nextcloud yet!')
            return False

        # Parse statistics
        conversion_stats = self.stats_parser.parse_conversion_stats(lims, run_dir, project_id)
        pid_lanes = self.stats_parser.parse_samplesheet(run_dir, project_id)
        summary_stats = self.stats_parser.parse_summary_stats(run_dir, pid_lanes)

        if not summary_stats:
            print(f'Error: No summary stats could be found for {run_dir}!')
            return False

        table_rows = [
            ['Data', 'Project (ID:Name)', 'Client Email', '# Samples', 'Total Reads', '% Q30', 'Mean Quality']
        ]
        if conversion_stats:
            table_rows.append([
                run_dir.name,
                f"{project_id}:{project.name}",
                researcher.email,
                len(conversion_stats['samples']),
                f"{conversion_stats['total_reads']:,.0f}",
                f"{conversion_stats['total_q30']:.1f}%",
                f"{conversion_stats['total_mean_qual']:.1f}"
            ])
        else:
            table_rows.append([

                    run_dir.name,
                    f"{project_id}:{project.name}",
                    researcher.email,
                    '?', '?', '?', '?'

            ])

        # Display confirmation table
        print("\nIllumina data sharing confirmation:")
        self._display_texttable(
            table_rows
        )

        if not query_yes_no("Are you sure you want to send this dataset"):
            return False

        # Get file list and share
        file_list = self.nextcloud_util.simple_file_list(nextcloud_run_id)
        if not file_list:
            print(f"Error: No files found in nextcloud directory {nextcloud_run_id}!")
            return False

        # Create available files list
        available_files_path = run_dir / f'available_files_{project_id}.txt'
        with open(available_files_path, 'w') as f:
            f.write('\n'.join(file_list))

        # Share data
        share_response = self.nextcloud_util.share(nextcloud_run_id, researcher.email)
        if "ERROR" in share_response:
            print(f"Error sharing: {share_response['ERROR']}")
            return False


        # Send notification
        self._send_illumina_notification(
            researcher, project_id, share_response, file_list,
            conversion_stats, get_sample_measurements(lims, project_id),
            lims.get_samples(projectlimsid=project_id), available_files_path
        )

        # Update database
        self._update_illumina_stats(
            session, IlluminaStats, flowcell_id, portal_run, project_id,
            run_dir, run_meta, conversion_stats, summary_stats
        )

        return True

    def _update_illumina_stats(self, session, IlluminaStats, flowcell_id, portal_run, project_id,
                             run_dir, run_meta, conversion_stats, summary_stats):
        """
        Internal method to update the portal database with Illumina sequencing statistics.

        1. Checks for existing records to prevent duplicate entries for the same flowcell/run.
        2. If conversion stats are provided, it stages a JSON metadata file and copies associated QC plot images (.png) to a temporary directory.
        3. Synchronizes these files to the portal storage server using rsync.
        4. Creates and commits a new `IlluminaStats` record containing primary metrics (yield, reads, Q30, etc.) and links to the uploaded plot filenames.

        Args:
            session: SQLAlchemy or equivalent database session for transactions.
            IlluminaStats: The database model class for Illumina statistics.
            flowcell_id (str): The flowcell ID.
            portal_run: The portal run object/record to which these stats will be linked.
            project_id (str): LIMS project ID.
            run_dir (Path): The local directory path containing the Illumina run data.
            run_meta (dict): Metadata from the run (e.g., start date, loading concentration).
            conversion_stats (dict): Statistics derived from BCL conversion (e.g., Q-scores).
            summary_stats (dict): Aggregated summary metrics (e.g., yields, cluster density).

        Raises:
            OSError: If directory creation, file copying, or cleanup fails.

        """
        # Check if stats already exist
        existing_stats = session.query(IlluminaStats).filter_by(
            flowcell_id=flowcell_id, run_id=portal_run.id
        ).first()

        if existing_stats:
            print(f'Warning: Stats for {flowcell_id} already exist in portal DB. Skipping.')
            return

        general_stats_filename = ''

        if conversion_stats:
            # Create and upload stats
            temp_dir = run_dir / flowcell_id
            temp_dir.mkdir(exist_ok=True)

            # Create JSON stats file
            stats_file = temp_dir / f'Conversion_Stats_{project_id}.json'
            with open(stats_file, 'w') as f:
                json.dump(conversion_stats, f, indent=2)

            # Copy plot files
            reports_dir = run_dir / "Conversion" / "Reports"
            if reports_dir.exists():
                os.system(f"cp {reports_dir}/*.png {temp_dir}")

            # Upload to portal storage
            rsync_command = (
                f"/usr/bin/rsync -rah {temp_dir} "
                f"{Config.PORTAL_USER}@{Config.PORTAL_SERVER}:{Config.PORTAL_STORAGE}/"
            )

            exit_code = os.system(rsync_command)
            if exit_code == 0:
                general_stats_filename = stats_file.name
                print(f'Uploaded stats for {project_id} to portal storage.')
            else:
                print(f'Error: Failed to copy stats to {Config.PORTAL_STORAGE}')

            os.system(f"rm -rf {temp_dir}")

        # Create database entry
        illumina_stats = IlluminaStats(
            flowcell_id=flowcell_id,
            general_stats=general_stats_filename,
            date=run_meta['date_started'],
            date_started=run_meta['date_started'],
            date_send=datetime.datetime.now(),
            run_name=run_dir.name,
            yield_r1=summary_stats.get('yield_r1', 0),
            yield_r2=summary_stats.get('yield_r2', 0),
            reads=summary_stats.get('reads', 0),
            avg_quality_r1=conversion_stats.get('avg_quality_r1', 0) if conversion_stats else 0,
            avg_quality_r2=conversion_stats.get('avg_quality_r2', 0) if conversion_stats else 0,
            cluster_density=summary_stats.get('cluster_density', 0),
            load_conc=run_meta.get('load_conc', 0),
            perc_q30_r1=summary_stats.get('perc_q30_r1', 0),
            perc_q30_r2=summary_stats.get('perc_q30_r2', 0),
            perc_occupied=summary_stats.get('perc_occupied', 0),
            phix_loaded=run_meta.get('phix_loaded', 0),
            phix_aligned=summary_stats.get('phix_aligned', 0),
            run_id=portal_run.id
        )

        # Add plot filenames if available
        if conversion_stats:
            plot_mapping = {
                'flowcell_intensity_plot': f'{run_dir.name}_flowcell-Intensity.png',
                'flowcell_density_plot': f'{run_dir.name}_Clusters-by-lane.png',
                'total_qscore_lanes_plot': f'{run_dir.name}_q-histogram.png',
                'cycle_qscore_lanes_plot': f'{run_dir.name}_q-heat-map.png',
                'cycle_base_plot': f'{run_dir.name}_BasePercent-by-cycle_BasePercent.png',
                'cycle_intensity_plot': f'{run_dir.name}_Intensity-by-cycle_Intensity.png',
            }

            for attr_name, filename in plot_mapping.items():
                setattr(illumina_stats, attr_name, filename)

        session.add(illumina_stats)
        session.commit()

        if general_stats_filename:
            print(f'Added stats record for {project_id} to portal DB.')
        else:
            print(f'Added minimal record for {project_id} to portal DB.')


def chunkify(lst: List, n: int) -> List:
    """
    Yield successive n-sized chunks from list.

    This is a generator function that splits a larger list into smaller sub-lists
    of a specified size. If the total length of the list is not evenly divisible
    by 'n', the final chunk will contain the remaining elements.

    Args:
        lst (List): The source list to be partitioned.
        n (int): The maximum size of each chunk.

    Yields:
        List: A sub-list containing up to 'n' elements from the original list.

    Example:
        >>> list(chunkify([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def run(lims, ids: Optional[str], username: Optional[str], directory: Optional[str],
        fid: Optional[str], all_dirs_ont: bool):
    """
    Main entry point for data sharing functionality.

    Args:
        lims: LIMS connection object.
        ids (Optional[str]): A comma-separated string of project IDs (e.g., 'PRJ1,PRJ2'). If provided, the script enters project-based sharing mode.
        username (Optional[str]): The username/email of the researcher for manual data sharing. Required if `ids` is not provided.
        directory (Optional[str]): The local path to the directory to be shared manually. Required if `ids` is not provided.
        fid (Optional[str]): An optional flowcell ID to override automatic detection.
        all_dirs_ont (bool): Nanopore-specific flag. If True, packages the entire data category folders. If False, attempts to package by barcode.

    Returns:
        bool: True if all specified project shares were attempted successfully (or
            if the manual share succeeded); False if parameters were missing or
            a fatal exception occurred.

    Raises:
        Exception: Catches and prints any unexpected errors occurring during the setup or sharing execution to ensure clean termination.

    """
    # Initialize NextCloud utility
    nextcloud_util = NextcloudUtil()
    nextcloud_util.set_hostname(Config.NEXTCLOUD_HOST)

    # Initialize data sharer
    data_sharer = DataSharer(nextcloud_util)

    try:
        if ids:
            # Share by project ID
            nextcloud_util.setup(
                Config.NEXTCLOUD_USER,
                Config.NEXTCLOUD_PW,
                Config.NEXTCLOUD_WEBDAV_ROOT,
                Config.NEXTCLOUD_RAW_DIR,
                Config.MAIL_SENDER
            )

            # Handle multiple project IDs
            project_ids = [pid.strip() for pid in ids.split(',')]
            results = []

            for project_id in project_ids:
                print(f"\nProcessing project {project_id}...")
                success = data_sharer.share_data_by_id(lims, project_id, fid, all_dirs_ont)
                results.append((project_id, success))

            # Summary
            print(f"\nSummary:")
            for project_id, success in results:
                status = "SUCCESS" if success else "FAILED"
                print(f"  {project_id}: {status}")

        elif username and directory:
            # Share by username and directory
            nextcloud_util.setup(
                Config.NEXTCLOUD_USER,
                Config.NEXTCLOUD_PW,
                Config.NEXTCLOUD_WEBDAV_ROOT,
                Config.NEXTCLOUD_MANUAL_DIR,
                Config.MAIL_SENDER
            )

            success = data_sharer.share_data_by_user(lims, username, directory)
            if success:
                print("Data sharing completed successfully.")
            else:
                print("Data sharing failed.")
        else:
            print("Error: Either provide project IDs or username+directory for sharing.")
            return False

    except Exception as e:

        print(f"Fatal error during data sharing: {e}")
        return False

    return True
