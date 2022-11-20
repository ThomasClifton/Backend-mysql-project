package projects.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import projects.entity.Category;
import projects.entity.Material;
import projects.entity.Project;
import projects.entity.Step;
import projects.exception.DbException;
import provided.util.DaoBase;

public class ProjectDao extends DaoBase {

  private static final String CATEGORY_TABLE = "category";
  private static final String MATERIAL_TABLE = "material";
  private static final String PROJECT_TABLE = "project";
  private static final String PROJECT_CATEGORY_TABLE = "project_category";
  private static final String STEP_TABLE = "step";

  /**
   * Inserts a new project into the project table
   * @param project Project
   * @return Project object with auto assigned projectId
   */
  public Project insertProject(Project project) {
    // @formatter:off
    String sql = ""
        + "INSERT INTO " + PROJECT_TABLE + " "
        + "(project_name, estimated_hours, actual_hours, difficulty, notes) "
        + "VALUES "
        + "(?, ?, ?, ?, ?)";
    // @formatter:on

    try (Connection conn = DbConnection.getConnection()) {
      startTransaction(conn);

      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        setParameter(stmt, 1, project.getProjectName(), String.class);
        setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
        setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
        setParameter(stmt, 4, project.getDifficulty(), Integer.class);
        setParameter(stmt, 5, project.getNotes(), String.class);

        stmt.executeUpdate();

        Integer projectId = getLastInsertId(conn, PROJECT_TABLE);

        commitTransaction(conn);

        project.setProjectId(projectId);
        return project;
      } 
      catch (Exception e) {
        rollbackTransaction(conn);
        throw new DbException(e);
      }
    } 
    catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Returns a List of all projects in the project table
   * @return List of Project
   */
  public List<Project> fetchAllProjects() {
    // @formatter:off
    String sql = ""
        + "SELECT * FROM "
        + PROJECT_TABLE
        + " ORDER BY project_name";
    // @formatter:on

    try (Connection conn = DbConnection.getConnection()) {
      startTransaction(conn);

      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<Project> projects = new LinkedList<>();
          while (rs.next()) {
            projects.add(extract(rs, Project.class));
          }
          return projects;
        }
      } 
      catch (Exception e) {
        rollbackTransaction(conn);
        throw new DbException(e);
      }
    } 
    catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Returns Project from project table based on projectId
   * @param projectId Integer
   * @return Project
   */
  public Optional<Project> fetchProjectById(Integer projectId) {
    String sql = "SELECT * FROM " + PROJECT_TABLE + " WHERE project_id = ?";

    try (Connection conn = DbConnection.getConnection()) {
      startTransaction(conn);

      try {
        Project project = null;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
          setParameter(stmt, 1, projectId, Integer.class);
          
          try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
              project = extract(rs, Project.class);
            }
          }
        }

        if (Objects.nonNull(project)) {
          project.getMaterials().addAll(fetchMaterialsForProject(conn, projectId));
          project.getSteps().addAll(fetchStepsForProject(conn, projectId));
          project.getCategories().addAll(fetchCategoriesForProject(conn, projectId));
        }

        commitTransaction(conn);

        return Optional.ofNullable(project);
      } 
      catch (Exception e) {
        rollbackTransaction(conn);
        throw new DbException(e);
      }
    } 
    catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Returns a list of materials from material table by projectId
   * @param conn Connection
   * @param projectId Integer
   * @return List of Material
   * @throws SQLException
   */
  private List<Material> fetchMaterialsForProject(Connection conn, Integer projectId)
      throws SQLException {
    // @formatter:off
    String sql = ""
        + "SELECT * FROM " + MATERIAL_TABLE + " "
        + "WHERE project_id = ?";
    // @formatter:on

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      setParameter(stmt, 1, projectId, Integer.class);
      try (ResultSet rs = stmt.executeQuery()) {
        List<Material> materials = new LinkedList<>();
        while (rs.next()) {
          materials.add(extract(rs, Material.class));
        }
        return materials;
      }
    }
  }
  
  /**
   * Returns a list of steps from step table by projectId
   * @param conn Connection
   * @param projectId Integer
   * @return List of Step
   * @throws SQLException
   */
  private List<Step> fetchStepsForProject(Connection conn, Integer projectId)
      throws SQLException {
    // @formatter:off
    String sql = ""
        + "SELECT * FROM " + STEP_TABLE + " "
        + "WHERE project_id = ?";
    // @formatter:on

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      setParameter(stmt, 1, projectId, Integer.class);
      try (ResultSet rs = stmt.executeQuery()) {
        List<Step> steps = new LinkedList<>();
        while (rs.next()) {
          steps.add(extract(rs, Step.class));
        }
        return steps;
      }
    }
  }
  
  /**
   * Returns a list of categories from category table by projectId
   * @param conn Connection
   * @param projectId Integer
   * @return List of Category
   * @throws SQLException
   */
  private List<Category> fetchCategoriesForProject(Connection conn, Integer projectId)
      throws SQLException {
    // @formatter:off
    String sql = ""
        + "SELECT c.* FROM " + CATEGORY_TABLE + " c "
        + "JOIN " + PROJECT_CATEGORY_TABLE + " pc USING (category_id)"
        + "WHERE project_id = ?";
    // @formatter:on

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      setParameter(stmt, 1, projectId, Integer.class);
      try (ResultSet rs = stmt.executeQuery()) {
        List<Category> categories = new LinkedList<>();
        while (rs.next()) {
          categories.add(extract(rs, Category.class));
        }
        return categories;
      }
    }
  }

  /**
   * Updates project in project table and returns true if one line is modified
   * @param project Project
   * @return boolean: true if 1 line modified, false otherwise
   */
  public boolean modifyProjectDetails(Project project) {
    // @formatter:off
    String sql = ""
        + "UPDATE " + PROJECT_TABLE + " SET "
        + "project_name = ?, "
        + "estimated_hours = ?, "
        + "actual_hours = ?, "
        + "difficulty = ?, "
        + "notes = ? "
        + "WHERE project_id = ?";
    // @formatter:on
    
    try (Connection conn = DbConnection.getConnection()) {
      startTransaction(conn);

      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        setParameter(stmt, 1, project.getProjectName(), String.class);
        setParameter(stmt, 2, project.getEstimatedHours(), BigDecimal.class);
        setParameter(stmt, 3, project.getActualHours(), BigDecimal.class);
        setParameter(stmt, 4, project.getDifficulty(), Integer.class);
        setParameter(stmt, 5, project.getNotes(), String.class);
        setParameter(stmt, 6, project.getProjectId(), Integer.class);

        boolean updated = stmt.executeUpdate() == 1;
        commitTransaction(conn);
        return updated;
      }
      catch (Exception e) {
        rollbackTransaction(conn);
        throw new DbException(e);
      }
    }
    catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Deletes project in project table and returns true if one line is deleted
   * @param projectId Integer
   * @return boolean: true if 1 line modified, false otherwise
   */
  public boolean deleteProject(Integer projectId) {
 // @formatter:off
    String sql = ""
        + "DELETE FROM " + PROJECT_TABLE + " "
        + "WHERE project_id = ?";
    // @formatter:on
    
    try (Connection conn = DbConnection.getConnection()) {
      startTransaction(conn);

      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        setParameter(stmt, 1, projectId, Integer.class);

        boolean deleted = stmt.executeUpdate() == 1;
        commitTransaction(conn);
        return deleted;
      }
      catch (Exception e) {
        rollbackTransaction(conn);
        throw new DbException(e);
      }
    }
    catch (SQLException e) {
      throw new DbException(e);
    }
  }
}
