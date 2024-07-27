import context
import webbrowser

from ataskq.executers.jenkins.jenkins_api import JenkinsClient


def main():
    client = JenkinsClient()
    build_number = client.run_jenkins_job("test", build="build")
    build_info = client.get_build_info("test", build_number)
    webbrowser.open(build_info["console"])


if __name__ == "__main__":
    main()
