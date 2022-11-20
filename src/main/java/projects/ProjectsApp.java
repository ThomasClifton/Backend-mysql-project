package projects;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import projects.entity.Project;
import projects.exception.DbException;
import projects.service.ProjectService;

public class ProjectsApp {

  // @formatter:off
  private List<String> operations = List.of(
      "1) Add a project",
      "2) List projects",
      "3) Select a project",
      "4) Update project details",
      "5) Delete a project"
  );
  // @formatter:on

  private Scanner scanner = new Scanner(System.in);
  
  private ProjectService projectService = new ProjectService();
  
  private Project curProject;

  public static void main(String[] args) {
    new ProjectsApp().processUserSelections();
  }

  /**
   * Handles Input given by user
   */
  private void processUserSelections() {
    boolean done = false;
    while (!done) {
      try {
        int selection = getUserSelection();
        switch (selection) {
          case -1:
            done = exitMenu();
            break;
          case 1:
            createProject();
            break;
          case 2:
            listProjects();
            break;
          case 3:
            selectproject();
            break;
          case 4:
            updateprojectDetails();
            break;
          case 5:
            deleteProject();
            break;
          default:
            System.out.println("\n" + selection + " is not a valid selection. Try again.");
            break;
        }
      } catch (Exception e) {
        System.out.println("\nError: " + e + " Try again.");
      }
    }

  }

  /**
   * Deletes a project in the project table based on input projectId
   */
  private void deleteProject() {
    listProjects();
    Integer projectId = getIntInput("Enter a project ID to delete");
    
    projectService.deleteProject(projectId);
    System.out.println("Project " + projectId + " was deleted successfully.");
    
    if(Objects.nonNull(curProject) && curProject.getProjectId().equals(projectId)) {
      curProject = null;
    }
  }

  /**
   * Creates new Project from user input then modifies existing project in project table
   */
  private void updateprojectDetails() {
    if(Objects.isNull(curProject)) {
      System.out.println("Please select a project");
      return;
    }
    
    Project project = new Project();
    
    String projectName = getStringInput("Enter the project name [" + curProject.getProjectName() + "]");
    BigDecimal estimatedHours = getDecimalInput("Enter the estimated hours [" + curProject.getEstimatedHours() + "]");
    BigDecimal actualHours = getDecimalInput("Enter the actual hours [" + curProject.getActualHours() + "]");
    Integer difficulty = getIntInput("Enter the project difficulty (1-5) [" + curProject.getDifficulty() + "]");
    String notes = getStringInput("Enter the project notes [" + curProject.getNotes() + "]");
    
    project.setProjectName(Objects.isNull(projectName) ? curProject.getProjectName() : projectName);
    project.setEstimatedHours(Objects.isNull(estimatedHours) ? curProject.getEstimatedHours() : estimatedHours);
    project.setActualHours(Objects.isNull(actualHours) ? curProject.getActualHours() : actualHours);
    project.setDifficulty(Objects.isNull(difficulty) ? curProject.getDifficulty() : difficulty);
    project.setNotes(Objects.isNull(notes) ? curProject.getNotes() : notes);
    project.setProjectId(curProject.getProjectId());
    
    projectService.modifyProjectDetails(project);
    curProject = projectService.fetchProjectById(curProject.getProjectId());
  }

  /**
   * Sets curProject based on user input
   */
  private void selectproject() {
    listProjects();
    Integer selection = getIntInput("Enter a project ID to select a project");
    curProject = null;
    curProject = projectService.fetchProjectById(selection);
    
    if(Objects.isNull(curProject)) {
      System.out.println("Invalid project ID selected.");
    }
  }

  /**
   * Prints all projects in project table
   */
  private void listProjects() {
    List<Project> projects = projectService.fetchAllProjects();
    System.out.println("\nProjects:");
    projects.forEach(project -> System.out.println("  " + project.getProjectId() + ": " + project.getProjectName()));
  }

  /**
   * Adds a new project to the project table
   */
  private void createProject() {
    String projectName = getStringInput("Enter the project name");
    BigDecimal estimatedHours = getDecimalInput("Enter the estimated hours");
    BigDecimal actualHours = getDecimalInput("Enter the actual hours");
    Integer difficulty = getIntInput("Enter the project difficulty (1-5)");
    String notes = getStringInput("Enter the project notes");
    
    Project project = new Project();
    
    project.setProjectName(projectName);
    project.setEstimatedHours(estimatedHours);
    project.setActualHours(actualHours);
    project.setDifficulty(difficulty);
    project.setNotes(notes);
    
    Project dbProject = projectService.addProject(project);
    System.out.println("You have successfully created project: " + dbProject);
  }

  /**
   * Returns true in order to exit menu
   * @return True
   */
  private boolean exitMenu() {
    System.out.println("Exiting the menu.");
    return true;
  }

  private int getUserSelection() {
    printOperations();
    Integer input = getIntInput("Enter a menu selection");
    return Objects.isNull(input) ? -1 : input;
  }

  private Integer getIntInput(String prompt) {
    String input = getStringInput(prompt);

    if (Objects.isNull(input)) {
      return null;
    }
    try {
      return Integer.valueOf(input);
    } catch (NumberFormatException e) {
      throw new DbException(input + " is not a valid number.");
    }
  }
  
  private BigDecimal getDecimalInput(String prompt) {
    String input = getStringInput(prompt);

    if (Objects.isNull(input)) {
      return null;
    }
    try {
      return new BigDecimal(input).setScale(2);
    } catch (NumberFormatException e) {
      throw new DbException(input + " is not a valid number.");
    }
  }

  private String getStringInput(String prompt) {
    System.out.print(prompt + ": ");
    String input = scanner.nextLine();

    return input.isBlank() ? null : input.trim();
  }

  private void printOperations() {
    System.out.println("\nThese are the available selections. Press the Enter key to quit:");
    operations.forEach(line -> System.out.println("  " + line));
    
    if(Objects.isNull(curProject)) {
      System.out.println("\nYou are not working with a project.");
    }
    else {
      System.out.println("\nYou are working with project: " + curProject);
    }
  }

}
