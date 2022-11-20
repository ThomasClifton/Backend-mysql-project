package projects.service;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import projects.dao.ProjectDao;
import projects.entity.Project;
import projects.exception.DbException;

public class ProjectService {
  
  private ProjectDao projectDao = new ProjectDao();

  /**
   * Adds a project to the project table
   * @param project Project
   **/
  public Project addProject(Project project) {
    return projectDao.insertProject(project);
  }

  /**
   * Fetches a List of Projects containing all projects in the table
   * @return List 
   */
  public List<Project> fetchAllProjects() {
    return projectDao.fetchAllProjects();
  }

  /**
   * Returns a Project found in the table based on the projectId
   * @param projectId Integer
   * @return Project
   */
  public Project fetchProjectById(Integer projectId) {
    return projectDao.fetchProjectById(projectId).orElseThrow(
        () -> new NoSuchElementException(
        "Project with project ID=" + projectId
        + " does not exist."));
  }

  /**
   * Modifies a project in the project table based on parameter project
   * @param project Project
   */
  public void modifyProjectDetails(Project project) {
    if(!projectDao.modifyProjectDetails(project)) {
      throw new DbException("Project with ID=" + project.getProjectId() + " does not exist.");
    }
  }

  /**
   * Deletes a project from the project table based on projectId
   * @param projectId Integer
   */
  public void deleteProject(Integer projectId) {
    if(!projectDao.deleteProject(projectId)) {
      throw new DbException("Project with ID=" + projectId + " does not exist.");
    }
  }

}
