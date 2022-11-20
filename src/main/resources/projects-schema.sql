DROP TABLE IF EXISTS material;

DROP TABLE IF EXISTS step;

DROP TABLE IF EXISTS project_category;

DROP TABLE IF EXISTS category;

DROP TABLE IF EXISTS project;

CREATE TABLE project (
  project_id INT AUTO_INCREMENT NOT NULL,
  project_name VARCHAR(128) NOT NULL,
  estimated_hours DECIMAL(7,2),
  actual_hours DECIMAL(7,2),
  difficulty INT,
  notes TEXT,
  PRIMARY KEY (project_id)
);

CREATE TABLE category (
  category_id INT AUTO_INCREMENT NOT NULL,
  category_name VARCHAR(128) NOT NULL,
  PRIMARY KEY (category_id)
);

CREATE TABLE project_category (
  project_id INT NOT NULL,
  category_id INT NOT NULL,
  FOREIGN KEY (project_id) REFERENCES project(project_id) ON DELETE CASCADE,
  FOREIGN KEY (category_id) REFERENCES category(category_id) ON DELETE CASCADE,
  UNIQUE KEY (project_id, category_id)
);

CREATE TABLE step (
  step_id INT AUTO_INCREMENT NOT NULL,
  project_id INT NOT NULL,
  step_text TEXT NOT NULL,
  step_order INT NOT NULL,
  PRIMARY KEY (step_id),
  FOREIGN KEY (project_id) REFERENCES project(project_id) ON DELETE CASCADE
);

CREATE TABLE material (
  material_id INT AUTO_INCREMENT NOT NULL,
  project_id INT NOT NULL,
  material_name VARCHAR(128) NOT NULL,
  num_required INT,
  cost DECIMAL(7,2),
  PRIMARY KEY (material_id),
  FOREIGN KEY (project_id) REFERENCES project(project_id) ON DELETE CASCADE
);

insert into project (project_name, estimated_hours, actual_hours, difficulty, notes)
values ('Door Hangers', 4.0, 3.0, 3, 'Hang the door using hinges from Home Depot');

INSERT INTO category (category_name) VALUES ('Doors and Windows');

INSERT INTO category (category_name) VALUES ('Repairs');

INSERT INTO material (project_id, material_name, num_required)
values (1, '2-inch screws', 20);

INSERT INTO material (project_id, material_name, num_required)
values (1, 'Hollow core door', 1);

INSERT INTO step (project_id, step_text, step_order)
values (1, 'Screw door hangers on the top and bottom of each side of the door frame', 1);

INSERT INTO step (project_id, step_text, step_order)
values (1, 'Screw door hangers on the top and bottom of each side of the door', 2);

INSERT INTO project_category (project_id, category_id)
values (1, 1);

INSERT INTO project_category (project_id, category_id)
values (1, 2);
